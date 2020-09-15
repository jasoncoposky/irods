
#include "experimental_plugin_framework.hpp"

#include "genquery_wrapper.hpp"
#include "genquery_stream_insertion.hpp"
#include "genquery_sql.hpp"

#include "nanodbc_connection.hpp"

namespace irods::experimental::api {

    class query : public base {
        public:
            query(const std::string& n) :
                base(n)
              , db_conn_(new_nanodbc_connection())
            {
            }

            virtual ~query()
            {
            }

        protected:

            auto operation(rsComm_t* comm, const json req) -> json
            {
                try {
                    auto psz = get<uint64_t>("page_size", req, MAX_SQL_SIZE);

                    if(!req.contains("paging")) {

                        auto [paging, sql] = generate_sql(req);
log::api::info("XXXX - sql {}", sql);

                        if(paging) {
                            begin();
                            one_shot(sql);
                            auto res  = fetch_page(psz);
                            auto rows = to_json(res);
                            blackboard.set({
                                    {"sql",     sql},
                                    {"results", rows},
                                    {"paging",  "true"},
                                    {constants::status, states::running}});
                        }
                        else {
                            auto res  = one_shot(sql);
                            auto rows = to_json(res);
                            blackboard.set({
                                    {"sql",     sql},
                                    {"results", rows},
                                    {constants::status, states::complete}});
                        }
                    }
                    else {
                        // fetch the next page in the query
                        auto res  = fetch_page(psz);
                        auto rows = to_json(res);
                        if(rows.size() > 0) {
                            blackboard.set({
                                    {"results", rows},
                                    {"paging",  "true"},
                                    {constants::status, states::running}});
                        }
                        else {
                            // no more results, were done
                            commit();
                            blackboard.set({
                                    {"results", {}},
                                    {constants::status, states::complete}});

                        }
                    } // else
                }
                catch(const std::exception& e) {
                    log::api::error(e.what());
                    blackboard.set({{constants::status,  states::failed},
                                    {constants::errors, {
                                    {constants::code,    SYS_INTERNAL_ERR},
                                    {constants::message, e.what()}}}});

                }
                catch(const irods::exception& e) {
                    log::api::error(e.what());
                    blackboard.set({{constants::status,  states::failed},
                                    {constants::errors, {
                                    {constants::code,    e.code()},
                                    {constants::message, e.what()}}}});
                }

                return blackboard.get();

            } // operation

        private:
            auto generate_sql(const json req) -> std::tuple<bool, std::string>
            {
                if(!req.contains("query")) {
                    THROW(SYS_INVALID_INPUT_PARAM, "Missing 'query' parameter");
                }

                auto gen = req.at("query").get<std::string>();
                auto sql = genquery::sql(genquery::wrapper::parse(gen));

                auto off = get<uint64_t>("offset", req, 0);
                if(off > 0) {
                    sql += " offset " + std::to_string(off);
                }

                auto lim = get<uint64_t>("limit", req, 0);
                if(lim > 0) {
                    sql += " limit " + std::to_string(lim);
                }

                bool paging{false};

                if(!off && !lim) {
                    paging = true;
                    sql = "DECLARE gq_cur CURSOR FOR " + sql;
                }

                sql += ";";

                return std::make_tuple(paging, sql);

            } // generate_sql

            auto begin() -> void
            {

                execute(db_conn_, "BEGIN;");
            }

            auto commit() -> void
            {
                execute(db_conn_, "COMMIT;");
            }

            auto fetch_page(uint64_t _ps) -> nanodbc::result
            {
                auto sql = fmt::format("FETCH {} FROM gq_cur;", _ps);
                return execute(db_conn_, sql);
            } // fetch_page

            auto one_shot(const std::string& _sql) -> nanodbc::result
            {
                return execute(db_conn_, _sql);
            } // one_shot

            auto to_json(nanodbc::result& _res) -> json
            {
                auto rows = json::array();

                for(auto r = 0; _res.next(); ++r) {
                    auto cols = json::array();

                    for(auto c = 0; c < _res.columns(); ++c) {
                        std::string x{};
                        _res.get_ref(c, {}, x);
                        cols.push_back(x);
                    }

                    rows.push_back(cols);

                } // for r

                return rows;

            } // to_json

            // private attributes
            nanodbc::connection db_conn_;

            const uint32_t MAX_SQL_SIZE{256};

    }; // class query

} // namespace irods::experimental::api

extern "C"
irods::experimental::api::query* plugin_factory(const std::string&, const std::string&) {
    return new irods::experimental::api::query{"query"};
}
