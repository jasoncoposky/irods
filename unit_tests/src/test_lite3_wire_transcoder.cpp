#include <catch2/catch_all.hpp>

#include "irods/wire_transcoder.hpp"
#include "irods/dataObjInpOut.h"
#include "irods/rodsGenQuery.h"
#include "irods/genquery2.h"
#include "irods/modAVUMetadata.h"
#include "irods/objStat.h"
#include "irods/rcMisc.h"
#include "irods/rcGlobalExtern.h"
#include "irods/irods_at_scope_exit.hpp"

#include <cstring>
#include <string_view>

TEST_CASE("irods::wire::negotiate_protocol version-aware negotiation")
{
    using irods::wire::negotiate_protocol;

    SECTION("Upgrades to LITE3_PROT when both client and server are >= rods6.0.0")
    {
        CHECK(negotiate_protocol("rods6.0.0", "rods6.0.0", NATIVE_PROT, false) == LITE3_PROT);
        CHECK(negotiate_protocol("rods6.1.0", "rods6.0.2", NATIVE_PROT, false) == LITE3_PROT);
        CHECK(negotiate_protocol("rods6.0.0", "rods6.0.0", LITE3_PROT, true) == LITE3_PROT);
    }

    SECTION("Preserves NATIVE_PROT or XML_PROT when communicating with 4.x or 5.x peers")
    {
        CHECK(negotiate_protocol("rods6.0.0", "rods4.3.2", NATIVE_PROT, false) == NATIVE_PROT);
        CHECK(negotiate_protocol("rods4.3.2", "rods6.0.0", NATIVE_PROT, false) == NATIVE_PROT);
        CHECK(negotiate_protocol("rods6.0.0", "rods5.0.0", NATIVE_PROT, false) == NATIVE_PROT);
        CHECK(negotiate_protocol("rods5.1.0", "rods6.0.0", XML_PROT, false) == XML_PROT);
        CHECK(negotiate_protocol("rods6.0.0", "rods5.0.0", LITE3_PROT, true) == NATIVE_PROT);
    }

    SECTION("Respects explicit XML_PROT, JSON_PROT, or environment override")
    {
        CHECK(negotiate_protocol("rods6.0.0", "rods6.0.0", XML_PROT, false) == XML_PROT);
        CHECK(negotiate_protocol("rods6.0.0", "rods6.0.0", JSON_PROT, false) == JSON_PROT);
        CHECK(negotiate_protocol("rods6.0.0", "rods6.0.0", NATIVE_PROT, true) == NATIVE_PROT);
    }
}

TEST_CASE("Edge Wire Transcoder cross-protocol round-trips")
{
    const irodsProt_t protocols[] = {NATIVE_PROT, XML_PROT, JSON_PROT, LITE3_PROT};

    SECTION("DataObjInp_PI and KeyValPair_PI round-trip across all 4 protocols")
    {
        dataObjInp_t inp{};
        rstrcpy(inp.objPath, "/tempZone/home/rods/lite3_test.txt", MAX_NAME_LEN);
        inp.createMode = 0640;
        inp.openFlags = 2;
        inp.offset = 4096;
        inp.dataSize = 1048576;
        inp.numThreads = 4;
        inp.oprType = 1;
        addKeyVal(&inp.condInput, "rescName", "demoResc");
        addKeyVal(&inp.condInput, "forceFlag", "");
        const auto clear_inp = irods::at_scope_exit{[&inp] { clearKeyVal(&inp.condInput); }};

        auto l3_res = irods::wire::struct_to_lite3(&inp, "DataObjInp_PI");
        REQUIRE(l3_res.has_value());
        CHECK(l3_res->get_str(0, "objPath") == "/tempZone/home/rods/lite3_test.txt");
        CHECK(l3_res->get_i64(0, "createMode") == 0640);
        CHECK(l3_res->get_i64(0, "dataSize") == 1048576);

        for (const auto prot : protocols) {
            auto wire_res = irods::wire::encode_from_lite3(*l3_res, "DataObjInp_PI", prot, "rods6.0.0");
            REQUIRE(wire_res.has_value());
            bytesBuf_t* wire_bbuf = *wire_res;
            const auto free_wire = irods::at_scope_exit{[&wire_bbuf] { freeBBuf(wire_bbuf); }};

            auto decoded_l3 = irods::wire::decode_to_lite3(
                {reinterpret_cast<const std::byte*>(wire_bbuf->buf), static_cast<std::size_t>(wire_bbuf->len)},
                "DataObjInp_PI",
                prot,
                "rods6.0.0");
            REQUIRE(decoded_l3.has_value());

            void* raw_out = nullptr;
            REQUIRE(irods::wire::lite3_to_struct(*decoded_l3, &raw_out, "DataObjInp_PI") == 0);
            REQUIRE(raw_out != nullptr);
            auto* out = static_cast<dataObjInp_t*>(raw_out);
            const auto free_out = irods::at_scope_exit{[&out] {
                clearKeyVal(&out->condInput);
                std::free(out);
            }};

            CHECK(std::string_view{out->objPath} == "/tempZone/home/rods/lite3_test.txt");
            CHECK(out->createMode == 0640);
            CHECK(out->openFlags == 2);
            CHECK(out->offset == 4096);
            CHECK(out->dataSize == 1048576);
            CHECK(out->numThreads == 4);
            CHECK(out->oprType == 1);
            REQUIRE(out->condInput.len == 2);
            CHECK(std::string_view{getValByKey(&out->condInput, "rescName")} == "demoResc");
            CHECK(getValByKey(&out->condInput, "forceFlag") != nullptr);
        }
    }

    SECTION("OpenedDataObjInp_PI and CollInpNew_PI round-trip via pack_struct / unpack_struct")
    {
        openedDataObjInp_t open_inp{};
        open_inp.l1descInx = 7;
        open_inp.len = 65536;
        open_inp.whence = 1;
        open_inp.oprType = 2;
        open_inp.offset = 128;
        open_inp.bytesWritten = 65536;
        addKeyVal(&open_inp.condInput, " verifyChksum", "sha256:abcd");
        const auto clear_open = irods::at_scope_exit{[&open_inp] { clearKeyVal(&open_inp.condInput); }};

        collInp_t coll_inp{};
        rstrcpy(coll_inp.collName, "/tempZone/home/rods/subcoll", MAX_NAME_LEN);
        coll_inp.flags = 1;
        coll_inp.oprType = 3;
        addKeyVal(&coll_inp.condInput, "recursiveOpr", "");
        const auto clear_coll = irods::at_scope_exit{[&coll_inp] { clearKeyVal(&coll_inp.condInput); }};

        for (const auto prot : protocols) {
            bytesBuf_t* packed_open = nullptr;
            REQUIRE(pack_struct(&open_inp, &packed_open, "OpenedDataObjInp_PI", RodsPackTable, 0, prot, "rods6.0.0") == 0);
            const auto free_packed_open = irods::at_scope_exit{[&packed_open] { freeBBuf(packed_open); }};

            void* unpacked_open_raw = nullptr;
            REQUIRE(unpack_struct(packed_open->buf, &unpacked_open_raw, "OpenedDataObjInp_PI", RodsPackTable, prot, "rods6.0.0") == 0);
            auto* unpacked_open = static_cast<openedDataObjInp_t*>(unpacked_open_raw);
            const auto free_unpacked_open = irods::at_scope_exit{[&unpacked_open] {
                clearKeyVal(&unpacked_open->condInput);
                std::free(unpacked_open);
            }};
            CHECK(unpacked_open->l1descInx == 7);
            CHECK(unpacked_open->len == 65536);
            CHECK(unpacked_open->bytesWritten == 65536);

            bytesBuf_t* packed_coll = nullptr;
            REQUIRE(pack_struct(&coll_inp, &packed_coll, "CollInpNew_PI", RodsPackTable, 0, prot, "rods6.0.0") == 0);
            const auto free_packed_coll = irods::at_scope_exit{[&packed_coll] { freeBBuf(packed_coll); }};

            void* unpacked_coll_raw = nullptr;
            REQUIRE(unpack_struct(packed_coll->buf, &unpacked_coll_raw, "CollInpNew_PI", RodsPackTable, prot, "rods6.0.0") == 0);
            auto* unpacked_coll = static_cast<collInp_t*>(unpacked_coll_raw);
            const auto free_unpacked_coll = irods::at_scope_exit{[&unpacked_coll] {
                clearKeyVal(&unpacked_coll->condInput);
                std::free(unpacked_coll);
            }};
            CHECK(std::string_view{unpacked_coll->collName} == "/tempZone/home/rods/subcoll");
            CHECK(unpacked_coll->flags == 1);
            CHECK(unpacked_coll->oprType == 3);
        }
    }

    SECTION("GenQueryInp_PI, GenQueryOut_PI, Genquery2Input_PI, ModAVUMetadataInp_PI, and RodsObjStat_PI round-trip")
    {
        genQueryInp_t gq_inp{};
        gq_inp.maxRows = 256;
        gq_inp.continueInx = 0;
        addInxIval(&gq_inp.selectInp, 501, 1);
        addInxVal(&gq_inp.sqlCondInp, 501, "= '/tempZone/home/rods'");
        const auto clear_gq_inp = irods::at_scope_exit{[&gq_inp] { clearGenQueryInp(&gq_inp); }};

        genQueryOut_t gq_out{};
        gq_out.rowCnt = 2;
        gq_out.attriCnt = 1;
        gq_out.continueInx = 0;
        gq_out.totalRowCount = 2;
        gq_out.sqlResult[0].attriInx = 501;
        gq_out.sqlResult[0].len = 32;
        gq_out.sqlResult[0].value = static_cast<char*>(std::calloc(2, 32));
        std::strcpy(gq_out.sqlResult[0].value, "/tempZone/home/rods/a");
        std::strcpy(gq_out.sqlResult[0].value + 32, "/tempZone/home/rods/b");
        const auto clear_gq_out = irods::at_scope_exit{[&gq_out] { std::free(gq_out.sqlResult[0].value); }};

        Genquery2Input gq2_inp{};
        gq2_inp.query_string = strdup("select COLL_NAME, DATA_NAME");
        gq2_inp.zone = strdup("tempZone");
        gq2_inp.sql_only = 0;
        const auto clear_gq2 = irods::at_scope_exit{[&gq2_inp] { clearGenquery2Input(&gq2_inp); }};

        modAVUMetadataInp_t avu_inp{};
        avu_inp.arg0 = const_cast<char*>("add");
        avu_inp.arg1 = const_cast<char*>("-d");
        avu_inp.arg2 = const_cast<char*>("/tempZone/home/rods/file.txt");
        avu_inp.arg3 = const_cast<char*>("attr_key");
        avu_inp.arg4 = const_cast<char*>("attr_val");
        avu_inp.arg5 = const_cast<char*>("attr_unit");

        rodsObjStat_t stat_inp{};
        stat_inp.objSize = 987654;
        stat_inp.objType = DATA_OBJ_T;
        stat_inp.dataMode = 0755;
        rstrcpy(stat_inp.dataId, "10042", NAME_LEN);
        rstrcpy(stat_inp.chksum, "sha2:xyz", NAME_LEN);
        rstrcpy(stat_inp.ownerName, "rods", NAME_LEN);
        rstrcpy(stat_inp.ownerZone, "tempZone", NAME_LEN);

        for (const auto prot : protocols) {
            // GenQueryInp_PI
            bytesBuf_t* b_gq_inp = nullptr;
            REQUIRE(pack_struct(&gq_inp, &b_gq_inp, "GenQueryInp_PI", RodsPackTable, 0, prot, "rods6.0.0") == 0);
            const auto f1 = irods::at_scope_exit{[&b_gq_inp] { freeBBuf(b_gq_inp); }};
            void* u_gq_inp_raw = nullptr;
            REQUIRE(unpack_struct(b_gq_inp->buf, &u_gq_inp_raw, "GenQueryInp_PI", RodsPackTable, prot, "rods6.0.0") == 0);
            auto* u_gq_inp = static_cast<genQueryInp_t*>(u_gq_inp_raw);
            const auto f2 = irods::at_scope_exit{[&u_gq_inp] {
                clearGenQueryInp(u_gq_inp);
                std::free(u_gq_inp);
            }};
            CHECK(u_gq_inp->maxRows == 256);
            REQUIRE(u_gq_inp->selectInp.len == 1);
            CHECK(u_gq_inp->selectInp.inx[0] == 501);
            REQUIRE(u_gq_inp->sqlCondInp.len == 1);
            CHECK(std::string_view{u_gq_inp->sqlCondInp.value[0]} == "= '/tempZone/home/rods'");

            // GenQueryOut_PI
            bytesBuf_t* b_gq_out = nullptr;
            REQUIRE(pack_struct(&gq_out, &b_gq_out, "GenQueryOut_PI", RodsPackTable, 0, prot, "rods6.0.0") == 0);
            const auto f3 = irods::at_scope_exit{[&b_gq_out] { freeBBuf(b_gq_out); }};
            void* u_gq_out_raw = nullptr;
            REQUIRE(unpack_struct(b_gq_out->buf, &u_gq_out_raw, "GenQueryOut_PI", RodsPackTable, prot, "rods6.0.0") == 0);
            auto* u_gq_out = static_cast<genQueryOut_t*>(u_gq_out_raw);
            const auto f4 = irods::at_scope_exit{[&u_gq_out] { freeGenQueryOut(&u_gq_out); }};
            REQUIRE(u_gq_out->rowCnt == 2);
            REQUIRE(u_gq_out->attriCnt == 1);
            CHECK(std::string_view{u_gq_out->sqlResult[0].value} == "/tempZone/home/rods/a");
            CHECK(std::string_view{u_gq_out->sqlResult[0].value + 32} == "/tempZone/home/rods/b");

            // Genquery2Input_PI
            bytesBuf_t* b_gq2 = nullptr;
            REQUIRE(pack_struct(&gq2_inp, &b_gq2, "Genquery2Input_PI", RodsPackTable, 0, prot, "rods6.0.0") == 0);
            const auto f5 = irods::at_scope_exit{[&b_gq2] { freeBBuf(b_gq2); }};
            void* u_gq2_raw = nullptr;
            REQUIRE(unpack_struct(b_gq2->buf, &u_gq2_raw, "Genquery2Input_PI", RodsPackTable, prot, "rods6.0.0") == 0);
            auto* u_gq2 = static_cast<Genquery2Input*>(u_gq2_raw);
            const auto f6 = irods::at_scope_exit{[&u_gq2] {
                clearGenquery2Input(u_gq2);
                std::free(u_gq2);
            }};
            CHECK(std::string_view{u_gq2->query_string} == "select COLL_NAME, DATA_NAME");
            CHECK(std::string_view{u_gq2->zone} == "tempZone");

            // ModAVUMetadataInp_PI
            bytesBuf_t* b_avu = nullptr;
            REQUIRE(pack_struct(&avu_inp, &b_avu, "ModAVUMetadataInp_PI", RodsPackTable, 0, prot, "rods6.0.0") == 0);
            const auto f7 = irods::at_scope_exit{[&b_avu] { freeBBuf(b_avu); }};
            void* u_avu_raw = nullptr;
            REQUIRE(unpack_struct(b_avu->buf, &u_avu_raw, "ModAVUMetadataInp_PI", RodsPackTable, prot, "rods6.0.0") == 0);
            auto* u_avu = static_cast<modAVUMetadataInp_t*>(u_avu_raw);
            const auto f8 = irods::at_scope_exit{[&u_avu] {
                clearModAVUMetadataInp(u_avu);
                std::free(u_avu);
            }};
            CHECK(std::string_view{u_avu->arg0} == "add");
            CHECK(std::string_view{u_avu->arg2} == "/tempZone/home/rods/file.txt");
            CHECK(std::string_view{u_avu->arg3} == "attr_key");
            CHECK(std::string_view{u_avu->arg4} == "attr_val");
            CHECK(std::string_view{u_avu->arg5} == "attr_unit");

            // RodsObjStat_PI
            bytesBuf_t* b_stat = nullptr;
            REQUIRE(pack_struct(&stat_inp, &b_stat, "RodsObjStat_PI", RodsPackTable, 0, prot, "rods6.0.0") == 0);
            const auto f9 = irods::at_scope_exit{[&b_stat] { freeBBuf(b_stat); }};
            void* u_stat_raw = nullptr;
            REQUIRE(unpack_struct(b_stat->buf, &u_stat_raw, "RodsObjStat_PI", RodsPackTable, prot, "rods6.0.0") == 0);
            auto* u_stat = static_cast<rodsObjStat_t*>(u_stat_raw);
            const auto f10 = irods::at_scope_exit{[&u_stat] { freeRodsObjStat(u_stat); }};
            CHECK(u_stat->objSize == 987654);
            CHECK(u_stat->objType == DATA_OBJ_T);
            CHECK(std::string_view{u_stat->dataId} == "10042");
            CHECK(std::string_view{u_stat->ownerName} == "rods");
        }
    }

    SECTION("Negative and edge-case bounds safety tests")
    {
        dataObjInp_t test_inp{};
        rstrcpy(test_inp.objPath, "/tempZone/home/rods/bounds.txt", MAX_NAME_LEN);
        test_inp.dataSize = 1024;

        auto l3_res = irods::wire::struct_to_lite3(&test_inp, "DataObjInp_PI", RodsPackTable, 0);
        REQUIRE(l3_res.has_value());

        // Bounds clamping in compute_lite3_buffer_size when a smaller max_bound is supplied
        const std::size_t exact_sz = irods::wire::compute_lite3_buffer_size(l3_res->data(), 1024 * 1024);
        CHECK(exact_sz == l3_res->size());
        const std::size_t clamped_sz = irods::wire::compute_lite3_buffer_size(l3_res->data(), 128);
        CHECK(clamped_sz <= 128);

        // Malformed JSON payload in decode_to_lite3 returns error cleanly
        const std::string bad_json = "{not valid json";
        const std::span<const std::byte> bad_json_span{
            reinterpret_cast<const std::byte*>(bad_json.data()), bad_json.size()};
        auto dec_bad =
            irods::wire::decode_to_lite3(bad_json_span, "DataObjInp_PI", JSON_PROT, "rods6.0.0", RodsPackTable);
        CHECK_FALSE(dec_bad.has_value());
    }
}
