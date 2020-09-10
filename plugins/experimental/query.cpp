
#include "experimental_plugin_framework.hpp"

#include "genquery_wrapper.hpp"
#include "genquery_stream_insertion.hpp"
#include "genquery_stringify.hpp"

namespace irods::experimental::api {

    class query : public base {
        public:
            query(const std::string& n) : base(n) {}
            virtual ~query() {}

        protected:
            bool enable_asynchronous_operation() { return true; }
        auto operation(rsComm_t* comm, const json req) -> json
        {
            try {
                blackboard.set({{constants::status, states::running}});

                auto query = req.at("query").get<std::string>();

                log::api::info("XXXX - query [{}]", query);

                auto result = stringify(Genquery::Wrapper::parse(query));

                log::api::info("SQL: [{}]", result);

                blackboard.set({
                        {"results", result},
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
