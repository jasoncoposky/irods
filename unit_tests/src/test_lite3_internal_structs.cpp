#include <catch2/catch_all.hpp>

#include "irods/protocol/lite3_structs.hpp"
#include "irods/irods_re_serialization.hpp"
#include "irods/irods_at_scope_exit.hpp"
#include "irods/rodsKeyWdDef.h"

#include <cstring>
#include <string_view>

TEST_CASE("irods::protocol::DataObjInp and KeyValPairView zero-copy B-Tree operations")
{
    irods::protocol::dataObjInp_t inp;
    inp.obj_path("/tempZone/home/rods/test_obj.txt");
    inp.create_mode(0600);
    inp.open_flags(O_CREAT | O_RDWR);
    inp.offset_val(4096);
    inp.data_size(1048576);
    inp.num_threads(4);
    inp.opr_type(PUT_OPR);

    auto cond = inp.cond_input();
    CHECK(cond.empty());
    cond[RESC_HIER_STR_KW] = "pt;ufs0";
    cond.set(DEST_RESC_NAME_KW, "pt");
    addKeyVal(&cond, FORCE_FLAG_KW, "");

    CHECK(inp.obj_path() == "/tempZone/home/rods/test_obj.txt");
    CHECK(inp.create_mode() == 0600);
    CHECK(inp.open_flags() == (O_CREAT | O_RDWR));
    CHECK(inp.offset_val() == 4096);
    CHECK(inp.data_size() == 1048576);
    CHECK(inp.num_threads() == 4);
    CHECK(inp.opr_type() == PUT_OPR);

    CHECK(cond.size() == 3);
    CHECK(cond.contains(RESC_HIER_STR_KW));
    CHECK(cond[RESC_HIER_STR_KW] == "pt;ufs0");
    CHECK(std::string_view{getValByKey(&cond, DEST_RESC_NAME_KW)} == "pt");
    CHECK(cond.contains(FORCE_FLAG_KW));

    // In-place mutation of existing key
    cond[RESC_HIER_STR_KW] = "pt;ufs1";
    CHECK(cond.get(RESC_HIER_STR_KW) == "pt;ufs1");

    // Contiguous value-semantic deep copy (O(1) single buffer copy, zero pointer chasing)
    irods::protocol::dataObjInp_t copy_inp = inp;
    CHECK(copy_inp.obj_path() == "/tempZone/home/rods/test_obj.txt");
    CHECK(copy_inp.cond_input().get(RESC_HIER_STR_KW) == "pt;ufs1");

    // Mutating copy does not affect original
    copy_inp.obj_path("/tempZone/home/rods/other.txt");
    copy_inp.cond_input().erase(DEST_RESC_NAME_KW);
    CHECK(inp.obj_path() == "/tempZone/home/rods/test_obj.txt");
    CHECK(inp.cond_input().contains(DEST_RESC_NAME_KW));
    CHECK_FALSE(copy_inp.cond_input().contains(DEST_RESC_NAME_KW));

    // Round-trip to legacy C ::dataObjInp_t and back
    auto legacy_res = inp.to_legacy();
    REQUIRE(legacy_res.has_value());
    ::dataObjInp_t* c_inp = *legacy_res;
    const auto free_c_inp = irods::at_scope_exit{[&c_inp] {
        clearKeyVal(&c_inp->condInput);
        std::free(c_inp->specColl);
        std::free(c_inp);
    }};
    CHECK(std::string_view{c_inp->objPath} == "/tempZone/home/rods/test_obj.txt");
    CHECK(c_inp->dataSize == 1048576);
    REQUIRE(getValByKey(&c_inp->condInput, RESC_HIER_STR_KW) != nullptr);
    CHECK(std::string_view{getValByKey(&c_inp->condInput, RESC_HIER_STR_KW)} == "pt;ufs1");

    // Cross-protocol wire encoding and decoding directly from DataObjInp
    for (const auto prot : {LITE3_PROT, JSON_PROT, XML_PROT, NATIVE_PROT}) {
        auto wire_res = inp.encode(prot, "rods6.0.0");
        REQUIRE(wire_res.has_value());
        auto dec_res = irods::protocol::DataObjInp::decode(*wire_res, prot, "rods6.0.0");
        REQUIRE(dec_res.has_value());
        CHECK(dec_res->obj_path() == "/tempZone/home/rods/test_obj.txt");
        CHECK(dec_res->data_size() == 1048576);
        CHECK(dec_res->cond_input().get(RESC_HIER_STR_KW) == "pt;ufs1");
        CHECK(dec_res->cond_input().get(DEST_RESC_NAME_KW) == "pt");
    }
}

TEST_CASE("irods::protocol::DataObjInfoList contiguous replica list & other facades")
{
    irods::protocol::DataObjInfoList replicas;
    for (int i = 0; i < 3; ++i) {
        irods::protocol::dataObjInfo_t info;
        info.obj_path("/tempZone/home/rods/replicated.bin");
        info.resc_name("resc" + std::to_string(i));
        info.resc_hier("comp;resc" + std::to_string(i));
        info.file_path("/vault/home/rods/replicated.bin." + std::to_string(i));
        info.data_size(65536);
        info.repl_num(i);
        info.repl_status(GOOD_REPLICA);
        info.data_id(50001);
        info.coll_id(10001);
        info.cond_input().set("replica_tag", std::to_string(i));
        replicas.push_back(std::move(info));
    }

    REQUIRE(replicas.size() == 3);
    CHECK(replicas[1].resc_hier() == "comp;resc1");
    CHECK(replicas[2].repl_num() == 2);

    // Bridge to legacy linked list and back
    auto legacy_head_res = replicas.to_legacy_list();
    REQUIRE(legacy_head_res.has_value());
    ::dataObjInfo_t* head = *legacy_head_res;
    const auto free_head = irods::at_scope_exit{[&head] { freeAllDataObjInfo(head); }};

    auto roundtrip_list_res = irods::protocol::DataObjInfoList::from_legacy_list(head);
    REQUIRE(roundtrip_list_res.has_value());
    REQUIRE(roundtrip_list_res->size() == 3);
    for (std::size_t i = 0; i < 3; ++i) {
        CHECK((*roundtrip_list_res)[i].obj_path() == "/tempZone/home/rods/replicated.bin");
        CHECK((*roundtrip_list_res)[i].repl_num() == static_cast<int>(i));
        CHECK((*roundtrip_list_res)[i].cond_input().get("replica_tag") == std::to_string(i));
    }

    // DataObjCopyInp nested views
    irods::protocol::dataObjCopyInp_t copy_op;
    copy_op.src().obj_path("/tempZone/home/rods/src.txt");
    copy_op.dest().obj_path("/tempZone/home/rods/dst.txt");
    copy_op.dest().cond_input().set(FORCE_FLAG_KW, "");
    CHECK(copy_op.src().obj_path() == "/tempZone/home/rods/src.txt");
    CHECK(copy_op.dest().obj_path() == "/tempZone/home/rods/dst.txt");
    CHECK(copy_op.dest().cond_input().contains(FORCE_FLAG_KW));

    // Dynamic PEP re_serialization directly from lite3_structs
    irods::re_serialization::serialized_parameter_t pep_map;
    irods::protocol::dataObjInp_t standalone_src = copy_op.src();
    irods::protocol::dataObjInp_t* standalone_ptr = &standalone_src;
    REQUIRE(irods::re_serialization::serialize_parameter(standalone_ptr, pep_map).ok());
    CHECK(pep_map["objPath"] == "/tempZone/home/rods/src.txt");

    // Zero-unpack bytesBuf_t API payload round-trip
    auto bbuf_res = standalone_src.encode_bbuf(LITE3_PROT, "rods6.0.0");
    REQUIRE(bbuf_res.has_value());
    bytesBuf_t* api_bbuf = *bbuf_res;
    const auto free_api_bbuf = irods::at_scope_exit{[&api_bbuf] { freeBBuf(api_bbuf); }};
    auto decoded_api_inp = irods::protocol::DataObjInp::decode_bbuf(*api_bbuf, LITE3_PROT, "rods6.0.0");
    REQUIRE(decoded_api_inp.has_value());
    CHECK(decoded_api_inp->obj_path() == "/tempZone/home/rods/src.txt");

    // Verify clear_spec_coll() -> spec_coll() and KeyValPair::clear() -> set()
    decoded_api_inp->spec_coll().collection("/tempZone/home/rods/mounted");
    CHECK(decoded_api_inp->has_spec_coll());
    CHECK(decoded_api_inp->spec_coll().collection() == "/tempZone/home/rods/mounted");
    decoded_api_inp->clear_spec_coll();
    CHECK_FALSE(decoded_api_inp->has_spec_coll());
    decoded_api_inp->spec_coll().collection("/tempZone/home/rods/remounted");
    CHECK(decoded_api_inp->has_spec_coll());
    CHECK(decoded_api_inp->spec_coll().collection() == "/tempZone/home/rods/remounted");

    irods::protocol::keyValPair_t standalone_kvp;
    standalone_kvp["k1"] = "v1";
    standalone_kvp["k2"] = "v2";
    CHECK(standalone_kvp.size() == 2);
    clearKeyVal(&standalone_kvp);
    CHECK(standalone_kvp.empty());
    addKeyVal(&standalone_kvp, "k3", "v3");
    CHECK(standalone_kvp.size() == 1);
    CHECK(standalone_kvp["k3"] == "v3");

    // Verify active_api_buffer_scope zero-copy LITE3_PROT request/reply pass-through
    {
        irods::protocol::active_api_buffer_scope scope{api_bbuf, LITE3_PROT};
        REQUIRE(irods::protocol::active_api_buffer_scope::has_active_request_buffer());

        auto active_req = irods::protocol::active_api_buffer_scope::try_get_active_request<irods::protocol::DataObjInp>();
        REQUIRE(active_req.has_value());
        CHECK(active_req->obj_path() == "/tempZone/home/rods/src.txt");

        irods::protocol::RodsObjStat stat_reply;
        stat_reply.obj_size(8192);
        stat_reply.obj_type(DATA_OBJ_T);
        stat_reply.data_id("42001");
        stat_reply.owner_name("rods");
        stat_reply.owner_zone("tempZone");
        irods::protocol::active_api_buffer_scope::set_active_reply(std::move(stat_reply));
        REQUIRE(irods::protocol::active_api_buffer_scope::has_active_reply_buffer());

        auto taken_reply = irods::protocol::active_api_buffer_scope::take_active_reply_buffer();
        REQUIRE(taken_reply.has_value());
        CHECK_FALSE(irods::protocol::active_api_buffer_scope::has_active_reply_buffer());

        irods::protocol::RodsObjStat verify_reply{std::move(*taken_reply)};
        CHECK(verify_reply.obj_size() == 8192);
        CHECK(verify_reply.data_id() == "42001");
        CHECK(verify_reply.owner_name() == "rods");
    }
    CHECK_FALSE(irods::protocol::active_api_buffer_scope::has_active_request_buffer());
}
