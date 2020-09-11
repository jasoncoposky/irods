
#include "experimental_plugin_framework.hpp"

#include "genquery_wrapper.hpp"
#include "genquery_stream_insertion.hpp"
#include "genquery_stringify.hpp"

#include "nanodbc_connection.hpp"

namespace irods::experimental::api {

    class query : public base {
        public:
            query(const std::string& n) : base(n) {}

            virtual ~query() {}

            const uint32_t MAX_SQL_SIZE = 256;

        protected:
            bool enable_asynchronous_operation() { return true; }

            auto operation(rsComm_t* comm, const json req) -> json
            {

                try {
                    blackboard.set({{constants::status, states::running}});

                    if(!req.contains("query")) {
                        THROW(SYS_INVALID_INPUT_PARAM, "Missing 'query' parameter");
                    }

                    auto gen = req.at("query").get<std::string>();
                    auto sql = stringify(Genquery::Wrapper::parse(gen));

                    auto off = get<uint64_t>("offset", req, 0);
                    if(off > 0) {
                        sql += " offset " + std::to_string(off);
                    }

                    auto lim = get<uint64_t>("limit", req, 0);
                    if(lim > 0) {
                        sql += " limit " + std::to_string(lim);
                    }

                    sql += ";";

log::api::info("XXXX - SQL [{}]", sql);

                    auto db_conn = new_nanodbc_connection();
                    auto results = execute(db_conn, sql);

                    auto rows = json::array();
                    for(auto r = 0; results.next(); ++r) {

                        auto arr = json::array();
                        arr.push_back(r);

                        for(auto c = 0; c < results.columns(); ++c) {
                            std::string col{};
                            results.get_ref(c, {}, col);
                            arr.push_back(col);
                        }

                        rows.push_back(arr);
                    } // for r

                    blackboard.set({
                            {"sql",     sql},
                            {"results", rows},
                            {constants::status, states::complete}});
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

    }; // class query

} // namespace irods::experimental::api

extern "C"
irods::experimental::api::query* plugin_factory(const std::string&, const std::string&) {
    return new irods::experimental::api::query{"query"};
}
