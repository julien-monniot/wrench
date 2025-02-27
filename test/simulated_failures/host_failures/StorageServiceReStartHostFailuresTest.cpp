/**
 * Copyright (c) 2017-2021. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <gtest/gtest.h>
#include <wrench-dev.h>

#include "../../include/TestWithFork.h"
#include "../../include/UniqueTmpPathPrefix.h"
#include <wrench/services/helper_services/service_termination_detector/ServiceTerminationDetector.h>

#include <memory>
#include "../failure_test_util/ResourceSwitcher.h"
#include "../failure_test_util/SleeperVictim.h"
#include "../failure_test_util/ComputerVictim.h"

WRENCH_LOG_CATEGORY(storage_service_start_restart_host_failures_test, "Log category for StorageServiceReStartHostFailuresTest");


class StorageServiceReStartHostFailuresTest : public ::testing::Test {

public:
    std::shared_ptr<wrench::Workflow> workflow;
    std::shared_ptr<wrench::StorageService> storage_service;

    void do_StorageServiceRestartTest_test();

protected:
    ~StorageServiceReStartHostFailuresTest() {
        workflow->clear();
    }

    StorageServiceReStartHostFailuresTest() {
        // Create the simplest workflow
        workflow = wrench::Workflow::createWorkflow();


        // up from 0 to 100, down from 100 to 200, up from 200 to 300, etc.
        std::string trace_file_content = "PERIODICITY 100\n"
                                         " 0 1\n"
                                         " 100 0";

        std::string trace_file_name = "host.trace";
        std::string trace_file_path = "/tmp/" + trace_file_name;

        FILE *trace_file = fopen(trace_file_path.c_str(), "w");
        fprintf(trace_file, "%s", trace_file_content.c_str());
        fclose(trace_file);

        // Create a platform file
        std::string xml = "<?xml version='1.0'?>"
                          "<!DOCTYPE platform SYSTEM \"https://simgrid.org/simgrid.dtd\">"
                          "<platform version=\"4.1\"> "
                          "   <zone id=\"AS0\" routing=\"Full\"> "
                          "       <host id=\"FailedHost\" speed=\"1f\" core=\"1\" > "
                          "          <disk id=\"large_disk1\" read_bw=\"1Bps\" write_bw=\"1Bps\">"
                          "             <prop id=\"size\" value=\"10000000B\"/>"
                          "             <prop id=\"mount\" value=\"/\"/>"
                          "          </disk>"
                          "       </host>"
                          "       <host id=\"FailedHostTrace\" speed=\"1f\" state_file=\"" +
                          trace_file_name + "\"  core=\"1\" > "
                                            "          <disk id=\"large_disk1\" read_bw=\"1Bps\" write_bw=\"1Bps\">"
                                            "             <prop id=\"size\" value=\"10000000B\"/>"
                                            "             <prop id=\"mount\" value=\"/\"/>"
                                            "          </disk>"
                                            "       </host>"
                                            "       <host id=\"StableHost\" speed=\"1f\" core=\"1\" > "
                                            "          <disk id=\"large_disk1\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                                            "             <prop id=\"size\" value=\"10000000B\"/>"
                                            "             <prop id=\"mount\" value=\"/\"/>"
                                            "          </disk>"
                                            "       </host>"
                                            "       <link id=\"link1\" bandwidth=\"1kBps\" latency=\"0\"/>"
                                            "       <route src=\"FailedHost\" dst=\"StableHost\">"
                                            "           <link_ctn id=\"link1\"/>"
                                            "       </route>"
                                            "       <route src=\"FailedHostTrace\" dst=\"StableHost\">"
                                            "           <link_ctn id=\"link1\"/>"
                                            "       </route>"
                                            "   </zone> "
                                            "</platform>";
        FILE *platform_file = fopen(platform_file_path.c_str(), "w");
        fprintf(platform_file, "%s", xml.c_str());
        fclose(platform_file);
    }

    std::string platform_file_path = UNIQUE_TMP_PATH_PREFIX + "platform.xml";
};


/**********************************************************************/
/**          START SERVICE ON DOWN HOST TEST                         **/
/**********************************************************************/

class StorageServiceRestartTestWMS : public wrench::ExecutionController {

public:
    StorageServiceRestartTestWMS(StorageServiceReStartHostFailuresTest *test,
                                 std::string &hostname) : wrench::ExecutionController(hostname, "test") {
        this->test = test;
    }

private:
    StorageServiceReStartHostFailuresTest *test;

    int main() override {

        // Starting a FailedHost murderer!!
        auto murderer = std::make_shared<wrench::ResourceSwitcher>("StableHost", 100, "FailedHost",
                                                                   wrench::ResourceSwitcher::Action::TURN_OFF, wrench::ResourceSwitcher::ResourceType::HOST);
        murderer->setSimulation(this->simulation);
        murderer->start(murderer, true, false);// Daemonized, no auto-restart

        // Starting a FailedHost resurector!!
        auto resurector = std::make_shared<wrench::ResourceSwitcher>("StableHost", 500, "FailedHost",
                                                                     wrench::ResourceSwitcher::Action::TURN_ON, wrench::ResourceSwitcher::ResourceType::HOST);
        resurector->setSimulation(this->simulation);
        resurector->start(murderer, true, false);// Daemonized, no auto-restart

        auto file = this->test->workflow->getFileByID("file");
        auto storage_service = this->test->storage_service;
        try {
            wrench::StorageService::readFileAtLocation(wrench::FileLocation::LOCATION(storage_service, file));
            throw std::runtime_error("Should not have been able to read the file (first attempt)");
        } catch (wrench::ExecutionException &e) {
            // Expected
        }
        wrench::Simulation::sleep(1000);
        try {
            wrench::StorageService::readFileAtLocation(wrench::FileLocation::LOCATION(storage_service, file));
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Should have been able to read the file (second attempt)");
        }

        // Starting a FailedHost murderer!!
        murderer = std::make_shared<wrench::ResourceSwitcher>("StableHost", 100, "FailedHost",
                                                              wrench::ResourceSwitcher::Action::TURN_OFF, wrench::ResourceSwitcher::ResourceType::HOST);
        murderer->setSimulation(this->simulation);
        murderer->start(murderer, true, false);// Daemonized, no auto-restart

        // Starting a FailedHost resurector!!
        resurector = std::make_shared<wrench::ResourceSwitcher>("StableHost", 500, "FailedHost",
                                                                wrench::ResourceSwitcher::Action::TURN_ON, wrench::ResourceSwitcher::ResourceType::HOST);
        resurector->setSimulation(this->simulation);
        resurector->start(murderer, true, false);// Daemonized, no auto-restart

        try {
            wrench::StorageService::readFileAtLocation(wrench::FileLocation::LOCATION(storage_service, file));
            throw std::runtime_error("Should not have been able to read the file (first attempt)");
        } catch (wrench::ExecutionException &e) {
            // Expected
        }
        wrench::Simulation::sleep(1000);
        try {
            wrench::StorageService::readFileAtLocation(wrench::FileLocation::LOCATION(storage_service, file));
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Should  have been able to read the file (second attempt)");
        }

        return 0;
    }
};

TEST_F(StorageServiceReStartHostFailuresTest, DISABLED_StorageServiceReStartTest) {
    DO_TEST_WITH_FORK(do_StorageServiceRestartTest_test);
}

void StorageServiceReStartHostFailuresTest::do_StorageServiceRestartTest_test() {

    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();
    int argc = 2;
    auto argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    argv[1] = strdup("--wrench-host-shutdown-simulation");
    //    argv[2] = strdup("--wrench-full-log");
    //    argv[3] = strdup("--log=root.thresh:debug");


    ASSERT_NO_THROW(simulation->init(&argc, argv));

    // Setting up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path));

    // Get a hostname
    std::string failed_host = "FailedHost";
    storage_service = simulation->add(wrench::SimpleStorageService::createSimpleStorageService(failed_host, {"/"},
                                                                                               {{wrench::SimpleStorageServiceProperty::BUFFER_SIZE, "10000000"}}));
    //                                                                                               {{wrench::SimpleStorageServiceProperty::BUFFER_SIZE, "0"}}));

    // Create a WMS
    std::string stable_host = "StableHost";
    std::shared_ptr<wrench::ExecutionController> wms = nullptr;

    ASSERT_NO_THROW(wms = simulation->add(
                            new StorageServiceRestartTestWMS(this, stable_host)));

    auto file = workflow->addFile("file", 10000000);

    simulation->add(new wrench::FileRegistryService(stable_host));

    simulation->stageFile(file, storage_service);

    // Running a "run a single task1" simulation
    ASSERT_NO_THROW(simulation->launch());


    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}
