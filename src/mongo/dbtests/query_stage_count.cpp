/**
 *    Copyright (C) 2013 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include <memory>

#include "mongo/db/dbdirectclient.h"
#include "mongo/db/exec/index_scan.h"
#include "mongo/db/exec/collection_scan.h"
#include "mongo/db/exec/collection_scan_common.h"
#include "mongo/db/exec/plan_stage.h"
#include "mongo/db/exec/working_set.h"
#include "mongo/db/exec/count.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/db/query/plan_executor.h"
#include "mongo/db/query/get_executor.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/dbtests/dbtests.h"

namespace QueryStageCount {

    const int kDocuments = 100;
    const int kInterjections = kDocuments;

    class CountStageTest {
    public:
        CountStageTest() : _client(&_txn) {}

        virtual ~CountStageTest() {}

        virtual void interject(Client::WriteContext&, CountStage&, int) {}

        virtual void setup() {
            _client.dropCollection(ns());
            _client.createCollection(ns());
            _client.ensureIndex(ns(), BSON("x" << 1));

            for (int i=0; i<kDocuments; i++) {
                insert(BSON("x" << i));
            }

        }

        void getLocs(Collection* collection) {
            _locs.clear();
            WorkingSet ws;

            CollectionScanParams params;
            params.collection = collection;
            params.direction = CollectionScanParams::FORWARD;
            params.tailable = false;

            scoped_ptr<CollectionScan> scan(new CollectionScan(&_txn, params, &ws, NULL));
            while (!scan->isEOF()) {
                WorkingSetID id = WorkingSet::INVALID_ID;
                PlanStage::StageState state = scan->work(&id);
                if (PlanStage::ADVANCED == state) {
                    WorkingSetMember* member = ws.get(id);
                    verify(member->hasLoc());
                    _locs.push_back(member->loc);
                }
            }
        }

        void insert(const BSONObj& doc) {
            _client.insert(ns(), doc);
        }

        void remove(const BSONObj& doc) {
            _client.remove(ns(), doc);
        }

        void update(const BSONObj& q, const BSONObj& u) {
            _client.update(ns(), q, u);
        }

        void testCount(const CountRequest& request, ssize_t expected_n=kDocuments, bool indexed=false) {
            setup();
            Client::WriteContext ctx(&_txn, ns());
            getLocs(ctx.getCollection());
            Collection* collection = ctx.getCollection();

            auto_ptr<WorkingSet> ws(new WorkingSet);

            StatusWithMatchExpression swme = MatchExpressionParser::parse(request.query);
            auto_ptr<MatchExpression> expression(swme.getValue());

            PlanStage* scan;
            if (indexed) {
                scan = createIndexScan(collection, expression.get(), ws.get());
            } else {
                scan = createCollScan(collection, expression.get(), ws.get());
            }

            CountStage countStage(&_txn, collection, request, ws.get(), scan);

            const CountStats* stats = runCount(countStage, ctx);

            ASSERT_FALSE(stats->trivialCount);
            ASSERT_EQUALS(stats->nCounted, expected_n);
            ASSERT_EQUALS(stats->nSkipped, request.skip);
            std::cout << "done" <<std::endl;
        }

        const CountStats* runCount(CountStage& count_stage, Client::WriteContext& ctx) {
            int interjection = 0;
            WorkingSetID wsid;

            while (!count_stage.isEOF()) {
                // do some work
                count_stage.work(&wsid);

                // prepare for yield
                count_stage.saveState();

                // interject in some way kInterjection times
                while (interjection < kInterjections) {
                    interject(ctx, count_stage, interjection);
                    interjection++;
                }

                // resume from yield
                count_stage.restoreState(&_txn);
            }

            return static_cast<const CountStats*>(count_stage.getSpecificStats());
        }

        IndexScan* createIndexScan(Collection* coll, MatchExpression* expr, WorkingSet* ws) {
            IndexCatalog* catalog = coll->getIndexCatalog();
            IndexDescriptor* descriptor = catalog->findIndexByKeyPattern(&_txn, BSON("x" << 1));

            IndexScanParams params;
            params.descriptor = descriptor;
            params.bounds.isSimpleRange = true;
            params.bounds.startKey = BSON("" << 0);
            params.bounds.endKey = BSON("" << kDocuments+1);
            params.bounds.endKeyInclusive = true;
            params.direction = 1;

            // This child stage gets owned and freed by its parent CountStage
            return new IndexScan(&_txn, params, ws, expr);
        }

        CollectionScan* createCollScan(Collection* coll, MatchExpression* expr, WorkingSet* ws) {
            CollectionScanParams params;
            params.collection = coll;

            // This child stage gets owned and freed by its parent CountStage
            return new CollectionScan(&_txn, params, ws, expr);
        }

        CountRequest createCountRequest(const BSONObj& filter, size_t skip=0, size_t limit=0) {
            CountRequest request;
            request.ns = ns();
            request.query = filter;
            request.limit = limit;
            request.skip = skip;
            request.explain = false;
            request.hint = BSONObj();
            return request;
        }

        static const char* ns() { return "unittests.QueryStageCount"; }

    protected:
        vector<DiskLoc> _locs;
        OperationContextImpl _txn;
        DBDirectClient _client;
    };

    class QueryStageCountNoChangeDuringYield : public CountStageTest {
    public:
        void run() {
            BSONObj filter = BSON("x" << LT << kDocuments/2);
            CountRequest request = createCountRequest(filter);
            testCount(request, kDocuments/2);
            testCount(request, kDocuments/2, true);
        }
    };

    class QueryStageCountYieldWithSkip : public CountStageTest {
    public:
        void run() {
            CountRequest request = createCountRequest(BSON("x" << GTE << 0), 2);
            testCount(request, kDocuments-2);
            testCount(request, kDocuments-2, true);
        }
    };

    class QueryStageCountYieldWithLimit : public CountStageTest {
    public:
        void run() {
            CountRequest request = createCountRequest(BSON("x" << GTE << 0), 0, 2);
            testCount(request, 2);
            testCount(request, 2, true);
        }
    };


    class QueryStageCountInsertDuringYield : public CountStageTest {
    public:
        void run() {
            // expected count would be 1 but we insert 100 new records while
            // we are doing work
            CountRequest request = createCountRequest(BSON("x" << 1));
            testCount(request, kInterjections+1);
            testCount(request, kInterjections+1, true);
        }

        // This is called 100 times as we scan the collection
        void interject(Client::WriteContext& ctx, CountStage&, int) {
            insert(BSON("x" << 1));
            ctx.commit();
        }
    };

    class QueryStageCountDeleteDuringYield : public CountStageTest {
    public:
        void run() {
            // expected count would be 99 but we delete the second record
            // after doing the first unit of work
            CountRequest request = createCountRequest(BSON("x" << GTE << 1));
            testCount(request, kDocuments-2);
            testCount(request, kDocuments-2, true);
        }

        // At the point which this is called we are in between counting the first + second record
        void interject(Client::WriteContext& ctx, CountStage& count_stage, int interjection) {
            if (interjection == 0) {
                // At this point, our first interjection, we've counted _locs[0]
                // and are about to count _locs[1]
                count_stage.invalidate(_locs[0], INVALIDATION_DELETION);
                remove(BSON("x" << 0));
                ctx.commit();

                count_stage.invalidate(_locs[1], INVALIDATION_DELETION);
                remove(BSON("x" << 1));
                ctx.commit();
            }
        }
    };

    class QueryStageCountRollingDeleteDuringYield : public CountStageTest {
    public:
        void run() {
            // expected count would be 100 but we always delete the next record
            CountRequest request = createCountRequest(BSON("x" << GTE << 0));
            testCount(request, 1);
            testCount(request, 1, true);
        }

        void interject(Client::WriteContext& ctx, CountStage& count_stage, int interjection) {
            if (interjection != 99) {
                count_stage.invalidate(_locs[interjection+1], INVALIDATION_DELETION);
                remove(BSON("x" << (interjection + 1)));
                ctx.commit();
            }
        }
    };


    class QueryStageCountUpdateDuringYield : public CountStageTest {
    public:
        void run() {
            // expected count would be kDocuments-2 but we update the first and second records
            // after doing the first unit of work
            CountRequest request = createCountRequest(BSON("x" << GTE << 2));
            testCount(request, kDocuments);
            testCount(request, kDocuments, true);
        }

        // At the point which this is called we are in between the first and second record
        void interject(Client::WriteContext& ctx, CountStage& count_stage, int interjection) {
            if (interjection == 0) {
                count_stage.invalidate(_locs[0], INVALIDATION_MUTATION);
                update(BSON("x" << 0), BSON("x" << 100));
                ctx.commit();

                count_stage.invalidate(_locs[1], INVALIDATION_MUTATION);
                update(BSON("x" << 1), BSON("x" << 100));
                ctx.commit();
            }
        }
    };

    class QueryStageCountMultiKeyDuringYield : public CountStageTest {
    public:
        void run() {
            // expected count would be 1 but we insert 100 new records while
            // we are doing work
            CountRequest request = createCountRequest(BSON("x" << 1));
            testCount(request, kDocuments+1, true); // only applies to indexed case
        }

        // This is called 100 times as we scan the collection
        void interject(Client::WriteContext& ctx, CountStage&, int) {
            // Should cause index to be converted to multikey
            insert(BSON("x" << BSON_ARRAY(1 << 2)));
            ctx.commit();
        }
    };

    class All : public Suite {
    public:
        All() : Suite("query_stage_count") {}

        void setupTests() {
            add<QueryStageCountNoChangeDuringYield>();
            add<QueryStageCountYieldWithSkip>();
            add<QueryStageCountYieldWithLimit>();
            add<QueryStageCountInsertDuringYield>();
            add<QueryStageCountDeleteDuringYield>();
            add<QueryStageCountRollingDeleteDuringYield>();
            add<QueryStageCountUpdateDuringYield>();
            add<QueryStageCountMultiKeyDuringYield>();
        }
    } QueryStageCountAll;

} // namespace QueryStageCount
