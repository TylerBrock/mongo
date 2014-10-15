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

    class CountStageTest {
    public:
        CountStageTest() : _client(&_txn) {}

        virtual ~CountStageTest() {}

        virtual void interject(Client::WriteContext&, CountStage&, size_t) {}
        virtual size_t interjections() { return 0; }

        virtual void setup() {
            Client::WriteContext ctx(&_txn, ns());

            ctx.db()->dropCollection(&_txn, ns());
            ctx.db()->createCollection(&_txn, ns());

            for (int i=0; i<100; i++) {
                insert(BSON("x" << i));
            }

            ctx.commit();

            getLocs(ctx.getCollection());
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

        void doCount(const BSONObj& filter, ssize_t expected_n=100, size_t skip=0) {
            size_t interjection = 0;

            Client::WriteContext ctx(&_txn, ns());
            Collection* collection = ctx.getCollection();

            auto_ptr<WorkingSet> ws(new WorkingSet);
            CollectionScan* cs = createCollectionScan(collection, filter, ws);
            CountRequest request = createCountRequest(filter, skip);

            CountStage count_stage(&_txn, collection, request, ws.get(), cs);

            WorkingSetID wsid;

            while (!count_stage.isEOF()) {
                // do some work
                count_stage.work(&wsid);

                // prepare for yield
                count_stage.saveState();

                // interject in some way n_interjections times
                while (interjection++ < interjections())
                    interject(ctx, count_stage, interjection);

                // restore from yield
                count_stage.restoreState(&_txn);
            }

            const CountStats* stats = static_cast<const CountStats*>(count_stage.getSpecificStats());

            ASSERT_EQUALS(stats->nCounted, expected_n);
        }

        CollectionScan* createCollectionScan(Collection* collection, const BSONObj& filter, const auto_ptr<WorkingSet>& ws){
            CollectionScanParams params;
            params.collection = collection;

            StatusWithMatchExpression swme = MatchExpressionParser::parse(filter);
            MatchExpression* expression(swme.getValue());
            return new CollectionScan(&_txn, params, ws.get(), expression);
        }

        CountRequest createCountRequest(const BSONObj& filter, size_t skip=0) {
            CountRequest request;
            request.ns = ns();
            request.query = filter;
            request.limit = 0;
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
            setup();
            BSONObj filter = BSON("x" << GT << 49);
            doCount(filter, 50);
        }
    };

    class QueryStageCountInsertDuringYield : public CountStageTest {
    public:
        void run() {
            setup();
            // expected count would be 1 but we insert 100 new records while
            // we are doing work
            doCount(BSON("x" << 1), 101);
        }

        // This is called 100 times as we scan the collection
        void interject(Client::WriteContext& ctx, CountStage&, size_t) {
            insert(BSON("x" << 1));
            ctx.commit();
        }

        size_t interjections() { return 100; }
    };

    class QueryStageCountDeleteDuringYield : public CountStageTest {
    public:
        void run() {
            setup();
            // expected count would be 99 but we delete the second record after doing
            // the first unit of work
            doCount(BSON("x" << GTE << 1), 98);
        }

        // At the point which this is called we are in between the first and second record
        void interject(Client::WriteContext& ctx, CountStage& count_stage, size_t interjection) {
            if (interjection == 1) {
                count_stage.invalidate(_locs[0], INVALIDATION_DELETION);
                remove(BSON("x" << 0));
                ctx.commit();
                count_stage.invalidate(_locs[1], INVALIDATION_DELETION);
                remove(BSON("x" << 1));
                ctx.commit();
            }
        }

        size_t interjections() { return 1; }
    };

    class QueryStageCountUpdateDuringYield : public CountStageTest {
    public:
        void run() {
            setup();
            // expected count would be 98 but we update the second record after doing
            // the first unit of work
            doCount(BSON("x" << GTE << 2), 100);
        }

        // At the point which this is called we are in between the first and second record
        void interject(Client::WriteContext& ctx, CountStage& count_stage, size_t) {
            update(BSON("x" << 0), BSON("x" << 100));
            count_stage.invalidate(_locs[0], INVALIDATION_MUTATION);
            ctx.commit();
            update(BSON("x" << 1), BSON("x" << 100));
            count_stage.invalidate(_locs[1], INVALIDATION_MUTATION);
            ctx.commit();
        }

        size_t interjections() { return 1; }
    };

    class QueryStageCountYieldWithSkip : public CountStageTest {
    public:
        void run() {
            setup();
        }

        // At the point which this is called we are in between the first and second record
        void interject(Client::WriteContext&, CountStage&, size_t) {
        }

        size_t interjections() { return 100; }
    };

    class QueryPlanExecutorTest : public CountStageTest {
    public:
        void run() {
            setup();
            CountRequest request;
            request.query = BSONObj();
            request.ns = ns();
            request.hint = BSONObj();
            request.skip = 0;
            request.limit = 0;

            AutoGetCollectionForRead ctx(&_txn, ns());
            Collection* collection = ctx.getCollection();

            PlanExecutor* rawExec;
            getExecutorCount(&_txn, collection, request, &rawExec);
            scoped_ptr<PlanExecutor> executor(rawExec);

            executor->setYieldPolicy(PlanExecutor::YIELD_MANUAL);

            Status execPlanStatus = executor->executePlan();

            CountStage* countStage = static_cast<CountStage*>(executor->getRootStage());

            const CountStats* countStats = static_cast<const CountStats*>(
                countStage->getSpecificStats());

            ASSERT_EQUALS(100, countStats->nCounted);
        }
    };

    class All : public Suite {
    public:
        All() : Suite("query_stage_count") {}

        void setupTests() {
            add<QueryStageCountNoChangeDuringYield>();
            add<QueryStageCountInsertDuringYield>();
            add<QueryStageCountDeleteDuringYield>();
            add<QueryStageCountUpdateDuringYield>();
            add<QueryPlanExecutorTest>();
        }
    } QueryStageCountAll;

} // namespace QueryStageCount
