/* Copyright (c) 2015 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"       //Has to be first, compiler complains
#include "TpcC.h"
#include "MockCluster.h"
#include "Util.h"

namespace RAMCloud {

class TpcCTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    BindTransport::BindSession* session1;
    BindTransport::BindSession* session2;
    BindTransport::BindSession* session3;
    Tub<TPCC::Driver> driver;

    TpcCTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , ramcloud()
        , session1(NULL)
        , session2(NULL)
        , session3(NULL)
        , driver()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.master.logBytes = 500 * 1024 * 1024;
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master1";
        config.maxObjectKeySize = 512;
        config.maxObjectDataSize = 1024;
        config.segmentSize = 128*1024;
        config.segletSize = 128*1024;
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master2";
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master3";
        cluster.addServer(config);
        ramcloud.construct(&context, "mock:host=coordinator");

        // Get pointers to the master sessions.
        Transport::SessionRef session =
                ramcloud->clientContext->transportManager->getSession(
                "mock:host=master1");
        session1 = static_cast<BindTransport::BindSession*>(session.get());
        session = ramcloud->clientContext->transportManager->getSession(
                "mock:host=master2");
        session2 = static_cast<BindTransport::BindSession*>(session.get());
        session = ramcloud->clientContext->transportManager->getSession(
                "mock:host=master3");
        session3 = static_cast<BindTransport::BindSession*>(session.get());

        driver.construct(ramcloud.get(), 1, 1);
        driver->initBenchmark();
    }

    DISALLOW_COPY_AND_ASSIGN(TpcCTest);
};

TEST_F(TpcCTest, initBenchmark) {
    TPCC::Warehouse w(1);

    Buffer buf;
    ramcloud->read(w.tid(), w.pKey(), w.pKeyLength(), &buf);
    w.parseBuffer(buf);

    printf("%s %s %s %s %f %lf\n", w.data.W_NAME, w.data.W_STREET_1,
                                 w.data.W_STREET_1, w.data.W_CITY,
                                 w.data.W_TAX, w.data.W_YTD);
    EXPECT_NE(0U, strlen(w.data.W_NAME));
}

TEST_F(TpcCTest, basic) {
    for (int i = 0; i < 10; ++i) {
        bool outcome;
        double latency = driver->txNewOrder(1U, &outcome);
        printf("latency of txNewOrder %lf, outcome:%d\n", latency, outcome);
    }
    for (int i = 0; i < 10; ++i) {
        bool outcome;
        double latency = driver->txPayment(1U, &outcome);
        printf("latency of txPayment %lf, outcome:%d\n", latency, outcome);
    }
    for (int i = 0; i < 10; ++i) {
        bool outcome;
        double latency = driver->txOrderStatus(1U, &outcome);
        printf("latency of txOrderStatus %lf, outcome:%d\n", latency, outcome);
    }
    for (int i = 0; i < 10; ++i) {
        bool outcome;
        double latency = driver->txDelivery(1U, &outcome);
        printf("latency of txDelivery %lf, outcome:%d\n", latency, outcome);
    }
    for (uint32_t D_ID = 1; D_ID <= 10; ++D_ID){
        bool outcome;
        double latency = driver->txStockLevel(1U, D_ID, &outcome);
        printf("latency of txStockLevel %lf, outcome:%d\n", latency, outcome);
    }
}

}  // namespace RAMCloud
