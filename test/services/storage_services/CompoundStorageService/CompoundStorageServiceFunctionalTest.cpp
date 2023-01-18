/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#include <gtest/gtest.h>

#include <wrench-dev.h>
#include "../../../include/TestWithFork.h"
#include "../../../include/UniqueTmpPathPrefix.h"

WRENCH_LOG_CATEGORY(Compound_storage_service_functional_test, "Log category for CompoundStorageServiceFunctionalTest");


class CompoundStorageServiceFunctionalTest : public ::testing::Test {

public:

    std::shared_ptr<wrench::Workflow> workflow;

    std::shared_ptr<wrench::DataFile> file_1;
    std::shared_ptr<wrench::DataFile> file_10;
    std::shared_ptr<wrench::DataFile> file_100;
    std::shared_ptr<wrench::DataFile> file_500;

    std::shared_ptr<wrench::SimpleStorageService> simple_storage_service_100 = nullptr;
    std::shared_ptr<wrench::SimpleStorageService> simple_storage_service_510 = nullptr;
    std::shared_ptr<wrench::SimpleStorageService> simple_storage_service_1000 = nullptr;
    std::shared_ptr<wrench::CompoundStorageService> compound_storage_service = nullptr;

    std::shared_ptr<wrench::ComputeService> compute_service = nullptr;

    void do_BasicFunctionality_test();

protected:
    ~CompoundStorageServiceFunctionalTest() {
        workflow->clear();
    }

    CompoundStorageServiceFunctionalTest() {

        workflow = wrench::Workflow::createWorkflow();

        // Create the files
        file_1 = workflow->addFile("file_1", 1.0);
        file_10 = workflow->addFile("file_10", 10.0);
        file_100 = workflow->addFile("file_100", 100.0);
        file_500 = workflow->addFile("file_500", 500.0);

        // Create a three-hosts platform file (2 for simple storage, one for Compound Storage)
        std::string xml = "<?xml version='1.0'?>"
                          "<!DOCTYPE platform SYSTEM \"https://simgrid.org/simgrid.dtd\">"
                          "<platform version=\"4.1\"> "
                          "   <zone id=\"AS0\" routing=\"Full\"> "
                          "       <host id=\"ComputeHost\" speed=\"1f\">"
                          "       </host>"
                          "       <host id=\"SimpleStorageHost0\" speed=\"1f\">"
                          "          <disk id=\"large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"100B\"/>"
                          "             <prop id=\"mount\" value=\"/disk100/\"/>"
                          "          </disk>"
                          "          <disk id=\"other_large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"510B\"/>"
                          "             <prop id=\"mount\" value=\"/disk510/\"/>"
                          "          </disk>"
                          "          <disk id=\"other_other_large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"1000B\"/>"
                          "             <prop id=\"mount\" value=\"/disk1000/\"/>"
                          "          </disk>"
                          "       </host>"
                          "       <host id=\"SimpleStorageHost1\" speed=\"1f\">"
                          "          <disk id=\"large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"100B\"/>"
                          "             <prop id=\"mount\" value=\"/disk100/\"/>"
                          "          </disk>"
                          "          <disk id=\"other_large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"510B\"/>"
                          "             <prop id=\"mount\" value=\"/disk510/\"/>"
                          "          </disk>"
                          "          <disk id=\"other_other_large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"1000B\"/>"
                          "             <prop id=\"mount\" value=\"/disk1000/\"/>"
                          "          </disk>"
                          "       </host>"
                          "       <host id=\"CompoundStorageHost\" speed=\"1f\">"
                          "       </host>"
                          "       <link id=\"Link1\" bandwidth=\"50MBps\" latency=\"150us\"/> "
                          "       <link id=\"Link2\" bandwidth=\"50MBps\" latency=\"150us\"/> "
                          "       <route src=\"CompoundStorageHost\" dst=\"SimpleStorageHost0\"><link_ctn id=\"Link1\"/></route> "
                          "       <route src=\"CompoundStorageHost\" dst=\"SimpleStorageHost1\"><link_ctn id=\"Link2\"/></route> "
                          "   </zone> "
                          "</platform>";
        FILE *platform_file = fopen(platform_file_path.c_str(), "w");
        fprintf(platform_file, "%s", xml.c_str());
        fclose(platform_file);
    }

    std::string platform_file_path = UNIQUE_TMP_PATH_PREFIX + "platform.xml";
};


/**********************************************************************/
/**  BASIC FUNCTIONALITY SIMULATION TEST                             **/
/**********************************************************************/

class CompoundStorageServiceBasicFunctionalityTestCtrl : public wrench::ExecutionController {

public:
    CompoundStorageServiceBasicFunctionalityTestCtrl(CompoundStorageServiceFunctionalTest *test,
                                                  std::string hostname) : wrench::ExecutionController(hostname, "test"), test(test) {
    }

private:
    CompoundStorageServiceFunctionalTest *test;

    int main() {

        // Retrieve internal SimpleStorageServices
        auto simple_storage_services = test->compound_storage_service->getAllServices();
        if (simple_storage_services.size() != 2) {
                throw std::runtime_error(
                    "There should be two SimpleStorageServices available from the CompoundStorage. Found " + std::to_string(simple_storage_services.size()) + " instead");
        }

        // Verify synchronous request for current free space (currently same as capacity, as no file has been placed on internal services)
        auto expected_capacity = std::map<std::string, double>(
            {{test->simple_storage_service_100->getName(), 100.0}, {test->simple_storage_service_510->getName(), 610.0}}
        );
        auto free_space = test->compound_storage_service->getFreeSpace();
        if (free_space != expected_capacity) {
            throw std::runtime_error("'Free Space' available to CompoundStorageService is incorrect");
        }

        // Verify that total space is correct
        auto capacity = test->compound_storage_service->getTotalSpace();
        if (capacity != expected_capacity) {
            throw std::runtime_error("'Total Space' available to CompoundStorageService is incorrect");
        }

        // Verify that compound storage service mount point is simply DEV_NULL
        auto mount_point = test->compound_storage_service->getMountPoint();
        if (mount_point != wrench::LogicalFileSystem::DEV_NULL + "/") {
            throw std::runtime_error("CompoundStorageService should have only one LogicalFileSystem::DEV_NULL fs.");
        }

        // We don't support getLoad or getFileLastWriteDate on CompoundStorageService yet (and won't ?)
        try {
            test->compound_storage_service->getLoad();
            throw std::runtime_error("CompoundStorageService doesn't have a getLoad() implemented");
        } catch (std::logic_error &e) {}

        {
            auto file_1_loc = wrench::FileLocation::LOCATION(test->compound_storage_service, test->file_1);
            try {
                test->compound_storage_service->getFileLastWriteDate(file_1_loc);
                throw std::runtime_error("CompoundStorageService doesn't have a getFileLastWriteDate() implemented");
            } catch (std::logic_error &e) {}
        }

        // CompoundStorageServer should never be a scratch space (at init or set as later)
        if (test->compound_storage_service->isScratch() == true) {
            throw std::runtime_error("CompoundStorageService should never have isScratch == true");
        }

        try {
            test->compound_storage_service->setScratch();
            throw std::runtime_error("CompoundStorageService can't be setup as a scratch space");
        } catch (std::logic_error &e) {}
   

        // Test multiple messages that should answer with a failure cause, and in turn generate an ExecutionException
        // on caller's side    

        auto file_1_loc_src = wrench::FileLocation::LOCATION(test->compound_storage_service, test->file_1);
        auto file_1_loc_dst = wrench::FileLocation::LOCATION(test->simple_storage_service_100, test->file_1);
     
        try {
            wrench::StorageService::copyFile(file_1_loc_src, file_1_loc_dst);
            throw std::runtime_error("Should not be able to copy file with a CompoundStorageService as src or dst");
        } catch (wrench::ExecutionException &) {}
        
        try {
            wrench::StorageService::deleteFile(file_1_loc_src);
            throw std::runtime_error("Should not be able to delete file from a CompoundStorageService");
        } catch (wrench::ExecutionException &) {}
    
        try {
            wrench::StorageService::readFile(file_1_loc_src);
            throw std::runtime_error("Should not be able to read file from a CompoundStorageService");
        } catch (wrench::ExecutionException &) {}

        try {
            wrench::StorageService::writeFile(file_1_loc_src);
            throw std::runtime_error("Should not be able to write file on a CompoundStorageService");
        } catch (wrench::ExecutionException &) {}

        // This one simply answers that the file was not found
        if (wrench::StorageService::lookupFile(file_1_loc_src))
            throw std::runtime_error("Should not be able to lookup file from a CompoundStorageService");
       
        
        return 0;
    }
};

TEST_F(CompoundStorageServiceFunctionalTest, BasicFunctionality) {
    DO_TEST_WITH_FORK(do_BasicFunctionality_test);
}

void CompoundStorageServiceFunctionalTest::do_BasicFunctionality_test() {

    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();

    int argc = 1;
    char **argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    //    argv[1] = strdup("--wrench-full-log");

    ASSERT_NO_THROW(simulation->init(&argc, argv));

    // Setting up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path));

    // Get a hostname
    auto compute = "ComputeHost";
    auto simple_storage0 = "SimpleStorageHost0";
    auto simple_storage1 = "SimpleStorageHost1";
    auto compound_storage = "CompoundStorageHost";

    // Create a Compute Service
    ASSERT_NO_THROW(compute_service = simulation->add(
                            new wrench::BareMetalComputeService(compute,
                                                                {std::make_pair(compute, std::make_tuple(wrench::ComputeService::ALL_CORES,
                                                                                                          wrench::ComputeService::ALL_RAM))},
                                                                {})));

    // Create some simple storage services
    ASSERT_NO_THROW(simple_storage_service_100 = simulation->add(
                        wrench::SimpleStorageService::createSimpleStorageService(simple_storage0, {"/disk100"},
                            {{wrench::SimpleStorageServiceProperty::BUFFER_SIZE, "10000"}}, {})));

    ASSERT_NO_THROW(simple_storage_service_510 = simulation->add(
                        wrench::SimpleStorageService::createSimpleStorageService(simple_storage1, {"/disk100", "/disk510"},
                            {{wrench::SimpleStorageServiceProperty::BUFFER_SIZE, "10000"}}, {})));

    // Create a bad Compound Storage Service (no storage services)
    ASSERT_THROW(compound_storage_service = simulation->add(
                    new wrench::CompoundStorageService(compound_storage, {})),
                 std::invalid_argument);

    // Create a bad Compound Storage Service (nullptr storage service)
    ASSERT_THROW(compound_storage_service = simulation->add(
                    new wrench::CompoundStorageService(compound_storage, {simple_storage_service_1000})),
                std::invalid_argument);

    // Create a bad Compound Storage Service with a bogus property
    ASSERT_THROW(compound_storage_service = simulation->add(
                    new wrench::CompoundStorageService(compound_storage, {simple_storage_service_100}, {{wrench::CompoundStorageServiceProperty::STORAGE_SELECTION_METHOD, "round_robin"}}, {})),
                 std::invalid_argument);

    // Create a valid Compound Storage Service with a bogus property
    ASSERT_NO_THROW(compound_storage_service = simulation->add(
                        new wrench::CompoundStorageService(compound_storage, {simple_storage_service_100, simple_storage_service_510})));

    // Create a Controler
    std::shared_ptr<wrench::ExecutionController> wms = nullptr;
    ASSERT_NO_THROW(wms = simulation->add(
                            new CompoundStorageServiceBasicFunctionalityTestCtrl(this, compound_storage)));

    // A bogus staging (can't use CompoundStorageService for staging)
    ASSERT_THROW(simulation->stageFile(file_10, compound_storage_service), std::invalid_argument);

    // Running a "run a single task1" simulation
    ASSERT_NO_THROW(simulation->launch());

    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}