/**
 * Copyright (c) 2018-2019. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <gtest/gtest.h>
#include <wrench-dev.h>
#include <numeric>
#include <thread>
#include <chrono>

#include "../../../include/TestWithFork.h"
#include "../../../include/UniqueTmpPathPrefix.h"

WRENCH_LOG_CATEGORY(htcondor_service_test, "Log category for HTCondorServiceTest");


class HTCondorServiceTest : public ::testing::Test {

public:
    std::shared_ptr<wrench::DataFile> input_file;
    std::shared_ptr<wrench::DataFile> input_file2;
    std::shared_ptr<wrench::DataFile> input_file3;
    std::shared_ptr<wrench::DataFile> output_file1;
    std::shared_ptr<wrench::DataFile> output_file2;
    std::shared_ptr<wrench::DataFile> output_file3;
    std::shared_ptr<wrench::WorkflowTask> task1;
    std::shared_ptr<wrench::WorkflowTask> task2;
    std::shared_ptr<wrench::WorkflowTask> task3;
    std::shared_ptr<wrench::WorkflowTask> task4;
    std::shared_ptr<wrench::WorkflowTask> task5;
    std::shared_ptr<wrench::WorkflowTask> task6;
    std::shared_ptr<wrench::WorkflowTask> task7;
    std::shared_ptr<wrench::HTCondorComputeService> htcondor_service = nullptr;
    std::shared_ptr<wrench::BareMetalComputeService> baremetal_service = nullptr;
    std::shared_ptr<wrench::StorageService> storage_service = nullptr;
    std::shared_ptr<wrench::StorageService> storage_service2 = nullptr;

    std::shared_ptr<wrench::Workflow> workflow;
    std::shared_ptr<wrench::Workflow> grid_workflow;

    void do_StandardJobTaskTest_test();
    void do_StandardJobTaskFailureTest_test();
    void do_StandardJobTaskAddComputeServiceTest_test();
    void do_PilotJobTaskTest_test();
    void do_SimpleServiceTest_test();
    void do_GridUniverseTest_test();
    void do_NoGridUniverseSupportTest_test();
    void do_NoNonGridUniverseSupportTest_test();
    void do_NoGridJobSupportTest_test();
    void do_NotEnoughResourcesTest_test();
    void do_ScratchTest_test();

protected:
    ~HTCondorServiceTest() {
        this->workflow->clear();
        this->grid_workflow->clear();
    }

    HTCondorServiceTest() {

        // Create the simplest workflow
        workflow = wrench::Workflow::createWorkflow();

        //Creating separate workflow for grid universe
        grid_workflow = wrench::Workflow::createWorkflow();


        // Create the files
        input_file = workflow->addFile("input_file", 10.0);
        input_file2 = grid_workflow->addFile("input_file2", 6500000000.0);
        input_file3 = grid_workflow->addFile("input_file3", 10.0);
        output_file1 = workflow->addFile("output_file1", 10.0);
        output_file2 = workflow->addFile("output_file2", 10.0);
        output_file3 = workflow->addFile("output_file3", 10.0);

        // Create the tasks
        task1 = workflow->addTask("task_1_10s_1core", 10.0, 1, 1, 0);
        task2 = workflow->addTask("task_2_10s_1core", 10.0, 1, 1, 0);
        task3 = workflow->addTask("task_3_10s_2cores", 10.0, 2, 2, 0);
        task4 = workflow->addTask("task_4_10s_2cores", 10.0, 2, 2, 0);
        task5 = workflow->addTask("task_5_30s_1_to_3_cores", 30.0, 1, 3, 0);
        task6 = workflow->addTask("task_6_10s_1_to_2_cores", 12.0, 1, 2, 0);
        task7 = grid_workflow->addTask("grid_task1", 10.0, 1, 1, 0);
        //task8 = grid_workflow->addTask("grid_task2", 10.0, 1, 1, 0);


        // Add file-task1 dependencies
        task1->addInputFile(input_file);
        task2->addInputFile(input_file);
        task3->addInputFile(input_file);
        task4->addInputFile(input_file);
        task5->addInputFile(input_file);
        task6->addInputFile(input_file);
        task7->addInputFile(input_file2);
        //task8->addInputFile(input_file3);

        task1->addOutputFile(output_file1);


        // Create a platform file
        std::string xml = "<?xml version='1.0'?>"
                          "<!DOCTYPE platform SYSTEM \"https://simgrid.org/simgrid.dtd\">"
                          "<platform version=\"4.1\"> "
                          "   <zone id=\"AS0\" routing=\"Full\"> "
                          "       <host id=\"DualCoreHost\" speed=\"1f\" core=\"2\" > "
                          "          <disk id=\"large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"100B\"/>"
                          "             <prop id=\"mount\" value=\"/\"/>"
                          "          </disk>"
                          "          <disk id=\"other_large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"1000000B\"/>"
                          "             <prop id=\"mount\" value=\"/scratch\"/>"
                          "          </disk>"
                          "          <disk id=\"other_other_large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"1000000B\"/>"
                          "             <prop id=\"mount\" value=\"/scratch2\"/>"
                          "          </disk>"
                          "       </host>"
                          "       <host id=\"QuadCoreHost\" speed=\"1f\" core=\"4\" > "
                          "          <disk id=\"large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"100B\"/>"
                          "             <prop id=\"mount\" value=\"/\"/>"
                          "          </disk>"
                          "          <disk id=\"other_large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"1000000B\"/>"
                          "             <prop id=\"mount\" value=\"/scratch\"/>"
                          "          </disk>"
                          "          <disk id=\"other_other_large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"1000000B\"/>"
                          "             <prop id=\"mount\" value=\"/scratch2\"/>"
                          "          </disk>"
                          "       </host>"
                          "       <link id=\"1\" bandwidth=\"5000GBps\" latency=\"0us\"/>"
                          "       <route src=\"DualCoreHost\" dst=\"QuadCoreHost\"> <link_ctn id=\"1\"/> </route>"
                          "   </zone> "
                          "</platform>";
        FILE *platform_file = fopen(platform_file_path.c_str(), "w");
        fprintf(platform_file, "%s", xml.c_str());
        fclose(platform_file);

        //Grid Universe Platform file.
        std::string xml1 = "<?xml version='1.0'?>"
                           "<!DOCTYPE platform SYSTEM \"https://simgrid.org/simgrid.dtd\">"
                           "<platform version=\"4.1\"> "
                           "   <zone id=\"AS0\" routing=\"Full\"> "
                           "       <host id=\"DualCoreHost\" speed=\"1f\" core=\"2\" > "
                           "          <disk id=\"large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                           "             <prop id=\"size\" value=\"100GB\"/>"
                           "             <prop id=\"mount\" value=\"/\"/>"
                           "          </disk>"
                           "          <disk id=\"scratch\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                           "             <prop id=\"size\" value=\"1000000GB\"/>"
                           "             <prop id=\"mount\" value=\"/scratch\"/>"
                           "          </disk>"
                           "          <disk id=\"other_scratch\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                           "             <prop id=\"size\" value=\"1000000GB\"/>"
                           "             <prop id=\"mount\" value=\"/scratch2\"/>"
                           "          </disk>"
                           "       </host>"
                           "       <host id=\"QuadCoreHost\" speed=\"1f\" core=\"4\" > "
                           "          <disk id=\"large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                           "             <prop id=\"size\" value=\"100GB\"/>"
                           "             <prop id=\"mount\" value=\"/\"/>"
                           "          </disk>"
                           "          <disk id=\"scratch\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                           "             <prop id=\"size\" value=\"1000000GB\"/>"
                           "             <prop id=\"mount\" value=\"/scratch\"/>"
                           "          </disk>"
                           "          <disk id=\"other_scratch\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                           "             <prop id=\"size\" value=\"1000000GB\"/>"
                           "             <prop id=\"mount\" value=\"/scratch2\"/>"
                           "          </disk>"
                           "       </host>"
                           "       <host id=\"BatchHost1\" speed=\"1f\" core=\"10\"> "
                           "          <disk id=\"large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                           "             <prop id=\"size\" value=\"100GB\"/>"
                           "             <prop id=\"mount\" value=\"/\"/>"
                           "          </disk>"
                           "          <disk id=\"scratch\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                           "             <prop id=\"size\" value=\"1000000GB\"/>"
                           "             <prop id=\"mount\" value=\"/scratch\"/>"
                           "          </disk>"
                           "          <disk id=\"other_scratch\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                           "             <prop id=\"size\" value=\"1000000GB\"/>"
                           "             <prop id=\"mount\" value=\"/scratch2\"/>"
                           "          </disk>"
                           "       </host>"
                           "       <host id=\"BatchHost2\" speed=\"1f\" core=\"10\"> "
                           "          <disk id=\"large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                           "             <prop id=\"size\" value=\"100GB\"/>"
                           "             <prop id=\"mount\" value=\"/\"/>"
                           "          </disk>"
                           "          <disk id=\"scratch\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                           "             <prop id=\"size\" value=\"1000000GB\"/>"
                           "             <prop id=\"mount\" value=\"/scratch\"/>"
                           "          </disk>"
                           "          <disk id=\"other_scratch\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                           "             <prop id=\"size\" value=\"1000000GB\"/>"
                           "             <prop id=\"mount\" value=\"/scratch2\"/>"
                           "          </disk>"
                           "       </host>"
                           "       <link id=\"1\" bandwidth=\"5000GBps\" latency=\"0us\"/>"
                           "       <link id=\"2\" bandwidth=\"5000GBps\" latency=\"0us\"/>"
                           "       <link id=\"3\" bandwidth=\"5000GBps\" latency=\"0us\"/>"
                           "       <link id=\"4\" bandwidth=\"5000GBps\" latency=\"0us\"/>"
                           "       <link id=\"5\" bandwidth=\"5000GBps\" latency=\"0us\"/>"
                           "       <link id=\"6\" bandwidth=\"5000GBps\" latency=\"0us\"/>"
                           "       <route src=\"DualCoreHost\" dst=\"QuadCoreHost\"> <link_ctn id=\"1\"/> </route>"
                           "       <route src=\"BatchHost1\" dst=\"BatchHost2\"> <link_ctn id=\"2\"/> </route>"
                           "       <route src=\"DualCoreHost\" dst=\"BatchHost1\"> <link_ctn id=\"3\"/> </route>"
                           "       <route src=\"DualCoreHost\" dst=\"BatchHost2\"> <link_ctn id=\"4\"/> </route>"
                           "       <route src=\"QuadCoreHost\" dst=\"BatchHost1\"> <link_ctn id=\"5\"/> </route>"
                           "       <route src=\"QuadCoreHost\" dst=\"BatchHost2\"> <link_ctn id=\"6\"/> </route>"
                           "   </zone> "
                           "</platform>";

        FILE *platform_file1 = fopen(platform_file_path1.c_str(), "w");
        fprintf(platform_file1, "%s", xml1.c_str());
        fclose(platform_file1);
    }

    std::string platform_file_path = UNIQUE_TMP_PATH_PREFIX + "platform.xml";
    std::string platform_file_path1 = UNIQUE_TMP_PATH_PREFIX + "platform1.xml";
};

/**********************************************************************/
/**  STANDARD JOB SUBMISSION TASK SIMULATION TEST ON ONE HOST        **/
/**********************************************************************/

class HTCondorStandardJobTestWMS : public wrench::ExecutionController {

public:
    HTCondorStandardJobTestWMS(HTCondorServiceTest *test,
                               std::string &hostname) : wrench::ExecutionController(hostname, "test") {
        this->test = test;
    }

private:
    HTCondorServiceTest *test;

    int main() override {
        // Create a data movement manager
        auto data_movement_manager = this->createDataMovementManager();

        // Create a job manager
        auto job_manager = this->createJobManager();

        // Create a job
        std::map<std::shared_ptr<wrench::DataFile>, std::shared_ptr<wrench::FileLocation>> file_locations;
        file_locations[this->test->input_file] = wrench::FileLocation::LOCATION(this->test->storage_service, this->test->input_file);
        file_locations[this->test->output_file1] = wrench::FileLocation::LOCATION(this->test->storage_service, this->test->output_file1);
        auto two_task_job = job_manager->createStandardJob(
                {this->test->task1},
                file_locations,
                {},
                {}, {});

        // Submit the job for execution with service-specific args, which is not allowed
        try {
            std::map<std::string, std::string> service_specific_args;
            service_specific_args[this->test->task1->getID()] = "2";
            job_manager->submitJob(two_task_job, this->test->htcondor_service, service_specific_args);
            throw std::runtime_error("Should not have been able to submit a job with service-specific args");
        } catch (std::invalid_argument &e) {
        }

        // Submit the job for execution
        try {
            job_manager->submitJob(two_task_job, this->test->htcondor_service);
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error(e.what());
        }

        // Wait for a workflow execution event
        std::shared_ptr<wrench::ExecutionEvent> event;
        try {
            event = this->waitForNextEvent();
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Error while getting an execution event: " + e.getCause()->toString());
        }

        if (not std::dynamic_pointer_cast<wrench::StandardJobCompletedEvent>(event)) {
            throw std::runtime_error("Unexpected workflow execution event: " + event->toString());
        }

        return 0;
    }
};

TEST_F(HTCondorServiceTest, HTCondorStandardJobTest) {
    DO_TEST_WITH_FORK(do_StandardJobTaskTest_test);
}

void HTCondorServiceTest::do_StandardJobTaskTest_test() {

    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    auto argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    //    argv[1] = strdup("--wrench-full-log");

    ASSERT_NO_THROW(simulation->init(&argc, argv));

    // Setting up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path));

    // Get a hostname
    std::string hostname = wrench::Simulation::getHostnameList()[0];

    // Create a Storage Service
    ASSERT_NO_THROW(storage_service = simulation->add(
                            wrench::SimpleStorageService::createSimpleStorageService(hostname, {"/"})));

    // Create a BareMetalComputeService
    std::string execution_host = wrench::Simulation::getHostnameList()[1];
    std::shared_ptr<wrench::BareMetalComputeService> baremetal_compute_service;
    ASSERT_NO_THROW(baremetal_compute_service = simulation->add(
                            new wrench::BareMetalComputeService(
                                    execution_host,
                                    {std::make_pair(
                                            execution_host,
                                            std::make_tuple(wrench::Simulation::getHostNumCores(execution_host),
                                                            wrench::Simulation::getHostMemoryCapacity(execution_host)))},
                                    "/scratch")));

    // Create a set of compute services for the HTCondorComputeService to use
    std::set<std::shared_ptr<wrench::ComputeService>> compute_services;
    compute_services.insert(baremetal_compute_service);

    // Create a HTCondor Service
    ASSERT_NO_THROW(htcondor_service = simulation->add(
                            new wrench::HTCondorComputeService(
                                    hostname, std::move(compute_services),
                                    {{wrench::HTCondorComputeServiceProperty::INSTANT_RESOURCE_AVAILABILITIES, "false"}})));

    // Coverage
    htcondor_service->supportsCompoundJobs();
    htcondor_service->supportsPilotJobs();
    htcondor_service->supportsStandardJobs();

    // Create a WMS
    std::shared_ptr<wrench::ExecutionController> wms = nullptr;

    ASSERT_NO_THROW(wms = simulation->add(
                            new HTCondorStandardJobTestWMS(this, hostname)));

    // Create a file registry
    ASSERT_NO_THROW(simulation->add(new wrench::FileRegistryService(hostname)));

    // Staging the input_file on the storage service
    ASSERT_NO_THROW(simulation->stageFile(input_file, storage_service));

    // Running a "run a single task1" simulation
    ASSERT_NO_THROW(simulation->launch());


    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}


/**************************************************************************/
/**  STANDARD JOB SUBMISSION TASK FAILUER SIMULATION TEST ON ONE HOST    **/
/**************************************************************************/

class HTCondorStandardJobFailureTestWMS : public wrench::ExecutionController {

public:
    HTCondorStandardJobFailureTestWMS(HTCondorServiceTest *test,
                                      std::string &hostname) : wrench::ExecutionController(hostname, "test") {
        this->test = test;
    }

private:
    HTCondorServiceTest *test;

    int main() override {
        // Create a data movement manager
        auto data_movement_manager = this->createDataMovementManager();

        // Create a job manager
        auto job_manager = this->createJobManager();

        // Create a job
        std::map<std::shared_ptr<wrench::DataFile>, std::shared_ptr<wrench::FileLocation>> file_locations;
        file_locations[this->test->input_file] = wrench::FileLocation::LOCATION(this->test->storage_service, this->test->input_file);
        file_locations[this->test->output_file1] = wrench::FileLocation::LOCATION(this->test->storage_service, this->test->output_file1);
        auto two_task_job = job_manager->createStandardJob(
                {this->test->task1},
                file_locations,
                {},
                {}, {});

        // Submit the job for execution with service-specific args, which is not allowed
        try {
            std::map<std::string, std::string> service_specific_args;
            service_specific_args[this->test->task1->getID()] = "2";
            job_manager->submitJob(two_task_job, this->test->htcondor_service, service_specific_args);
            throw std::runtime_error("Should not have been able to submit a job with service-specific args");
        } catch (std::invalid_argument &e) {
        }

        // Remove a needed file to force a failure
        this->test->storage_service->deleteFile(wrench::FileLocation::LOCATION(this->test->storage_service, this->test->input_file));

        // Submit the job for execution
        try {
            job_manager->submitJob(two_task_job, this->test->htcondor_service);
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error(e.what());
        }

        // Wait for a workflow execution event
        std::shared_ptr<wrench::ExecutionEvent> event;
        try {
            event = this->waitForNextEvent();
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Error while getting an execution event: " + e.getCause()->toString());
        }

        if (not std::dynamic_pointer_cast<wrench::StandardJobFailedEvent>(event)) {
            throw std::runtime_error("Unexpected workflow execution event: " + event->toString());
        }

        return 0;
    }
};

TEST_F(HTCondorServiceTest, HTCondorStandardJobFailureTest) {
    DO_TEST_WITH_FORK(do_StandardJobTaskFailureTest_test);
}

void HTCondorServiceTest::do_StandardJobTaskFailureTest_test() {

    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    auto argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    //    argv[1] = strdup("--wrench-full-log");

    ASSERT_NO_THROW(simulation->init(&argc, argv));

    // Setting up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path));

    // Get a hostname
    std::string hostname = wrench::Simulation::getHostnameList()[0];

    // Create a Storage Service
    ASSERT_NO_THROW(storage_service = simulation->add(
                            wrench::SimpleStorageService::createSimpleStorageService(hostname, {"/"})));

    // Create a BareMetalComputeService
    std::string execution_host = wrench::Simulation::getHostnameList()[1];
    std::shared_ptr<wrench::BareMetalComputeService> baremetal_compute_service;
    ASSERT_NO_THROW(baremetal_compute_service = simulation->add(
                            new wrench::BareMetalComputeService(
                                    execution_host,
                                    {std::make_pair(
                                            execution_host,
                                            std::make_tuple(wrench::Simulation::getHostNumCores(execution_host),
                                                            wrench::Simulation::getHostMemoryCapacity(execution_host)))},
                                    "/scratch")));

    // Create a set of compute services for the HTCondorComputeService to use
    std::set<std::shared_ptr<wrench::ComputeService>> compute_services;
    compute_services.insert(baremetal_compute_service);

    // Create a HTCondor Service
    ASSERT_NO_THROW(htcondor_service = simulation->add(
                            new wrench::HTCondorComputeService(
                                    hostname, std::move(compute_services),
                                    {})));

    // Create a WMS
    std::shared_ptr<wrench::ExecutionController> wms = nullptr;

    ASSERT_NO_THROW(wms = simulation->add(
                            new HTCondorStandardJobFailureTestWMS(this, hostname)));

    // Create a file registry
    ASSERT_NO_THROW(simulation->add(new wrench::FileRegistryService(hostname)));

    // Staging the input_file on the storage service
    ASSERT_NO_THROW(simulation->stageFile(input_file, storage_service));

    // Running a "run a single task1" simulation
    ASSERT_NO_THROW(simulation->launch());


    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}

/**********************************************************************/
/**  STANDARD JOB SUBMISSION TASK SIMULATION TEST ON ONE HOST        **/
/**  ADDING A COMPUTE SERVICE ON THE FLY                             **/
/**********************************************************************/

class HTCondorStandardJobAddComputeServiceTestWMS : public wrench::ExecutionController {

public:
    HTCondorStandardJobAddComputeServiceTestWMS(HTCondorServiceTest *test,
                                                std::string &hostname) : wrench::ExecutionController(hostname, "test") {
        this->test = test;
    }

private:
    HTCondorServiceTest *test;

    int main() override {
        // Create a data movement manager
        auto data_movement_manager = this->createDataMovementManager();

        // Create a job manager
        auto job_manager = this->createJobManager();

        // Get a reference to the CondorHT
        auto htcondor_cs = this->test->htcondor_service;
        auto bm_cs = this->test->baremetal_service;

        // Add the bm_cs to the htcondor_cs
        htcondor_cs->addComputeService(bm_cs);

        // Create a job
        std::map<std::shared_ptr<wrench::DataFile>, std::shared_ptr<wrench::FileLocation>> file_locations;
        file_locations[this->test->input_file] = wrench::FileLocation::LOCATION(this->test->storage_service, this->test->input_file);
        file_locations[this->test->output_file1] = wrench::FileLocation::LOCATION(this->test->storage_service, this->test->output_file1);
        auto two_task_job = job_manager->createStandardJob(
                {this->test->task1},
                file_locations,
                {}, {}, {});


        // Submit the job for execution
        try {
            job_manager->submitJob(two_task_job, this->test->htcondor_service);
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error(e.what());
        }

        // Wait for a workflow execution event
        std::shared_ptr<wrench::ExecutionEvent> event;
        try {
            event = this->waitForNextEvent();
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Error while getting an execution event: " + e.getCause()->toString());
        }

        if (not std::dynamic_pointer_cast<wrench::StandardJobCompletedEvent>(event)) {
            throw std::runtime_error("Unexpected workflow execution event: " + event->toString());
        }

        return 0;
    }
};

TEST_F(HTCondorServiceTest, HTCondorStandardJobAddComputeServiceTest) {
    DO_TEST_WITH_FORK(do_StandardJobTaskAddComputeServiceTest_test);
}

void HTCondorServiceTest::do_StandardJobTaskAddComputeServiceTest_test() {

    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    auto argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");

    ASSERT_NO_THROW(simulation->init(&argc, argv));

    // Setting up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path));

    // Get a hostname
    std::string hostname = wrench::Simulation::getHostnameList()[0];

    // Create a Storage Service
    ASSERT_NO_THROW(storage_service = simulation->add(
                            wrench::SimpleStorageService::createSimpleStorageService(hostname, {"/"})));

    // Create a BareMetalComputeService
    std::string execution_host = wrench::Simulation::getHostnameList()[1];
    std::shared_ptr<wrench::BareMetalComputeService> baremetal_compute_service;
    ASSERT_NO_THROW(baremetal_compute_service = simulation->add(
                            new wrench::BareMetalComputeService(
                                    execution_host,
                                    {std::make_pair(
                                            execution_host,
                                            std::make_tuple(wrench::Simulation::getHostNumCores(execution_host),
                                                            wrench::Simulation::getHostMemoryCapacity(execution_host)))},
                                    "/scratch")));

    // Create a HTCondor Service
    ASSERT_NO_THROW(htcondor_service = simulation->add(
                            new wrench::HTCondorComputeService(
                                    hostname, {baremetal_compute_service},
                                    {})));

    // Create a WMS
    std::shared_ptr<wrench::ExecutionController> wms = nullptr;

    ASSERT_NO_THROW(wms = simulation->add(
                            new HTCondorStandardJobAddComputeServiceTestWMS(this, hostname)));

    // Create a file registry
    ASSERT_NO_THROW(simulation->add(new wrench::FileRegistryService(hostname)));

    // Staging the input_file on the storage service
    ASSERT_NO_THROW(simulation->stageFile(input_file, storage_service));

    // Running a "run a single task1" simulation
    ASSERT_NO_THROW(simulation->launch());


    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}

/**********************************************************************/
/**  PILOT JOB SUBMISSION TASK SIMULATION TEST ON ONE HOST        **/
/**********************************************************************/

class HTCondorPilotJobTestWMS : public wrench::ExecutionController {

public:
    HTCondorPilotJobTestWMS(HTCondorServiceTest *test,
                            std::string &hostname) : wrench::ExecutionController(hostname, "test") {
        this->test = test;
    }

private:
    HTCondorServiceTest *test;

    int main() override {
        // Create a data movement manager
        auto data_movement_manager = this->createDataMovementManager();

        // Create a job manager
        auto job_manager = this->createJobManager();

        // Submit a pilot job
        auto pilot_job = job_manager->createPilotJob();
        std::map<std::string, std::string> service_specific_arguments;
        service_specific_arguments["-N"] = "1";
        service_specific_arguments["-c"] = "1";
        service_specific_arguments["-t"] = "3600";
        service_specific_arguments["-universe"] = "grid";

        job_manager->submitJob(pilot_job, this->test->htcondor_service, service_specific_arguments);

        {
            // Wait for the job start
            auto event = this->waitForNextEvent();
            auto real_event = std::dynamic_pointer_cast<wrench::PilotJobStartedEvent>(event);
            if (not real_event) {
                throw std::runtime_error("Did not get the expected PilotJobStartEvent (got " + event->toString() + ")");
            }
        }

        // Create a job
        auto two_task_job = job_manager->createStandardJob(
                {this->test->task1},
                (std::map<std::shared_ptr<wrench::DataFile>, std::shared_ptr<wrench::FileLocation>>){},
                {std::make_tuple(
                        wrench::FileLocation::LOCATION(this->test->storage_service, this->test->input_file),
                        wrench::FileLocation::SCRATCH(this->test->input_file))},
                {}, {});

        // Submit the job for execution
        try {
            job_manager->submitJob(two_task_job, pilot_job->getComputeService());
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error(e.what());
        }

        // Wait for a workflow execution event
        std::shared_ptr<wrench::ExecutionEvent> event;
        try {
            event = this->waitForNextEvent();
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Error while getting an execution event: " + e.getCause()->toString());
        }

        if (not std::dynamic_pointer_cast<wrench::StandardJobCompletedEvent>(event)) {
            throw std::runtime_error("Unexpected workflow execution event: " + event->toString());
        }

        return 0;
    }
};

TEST_F(HTCondorServiceTest, HTCondorPilotJobTest) {
    DO_TEST_WITH_FORK(do_PilotJobTaskTest_test);
}

void HTCondorServiceTest::do_PilotJobTaskTest_test() {

    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    auto argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    //    argv[1] = strdup("--wrench-full-log");

    ASSERT_NO_THROW(simulation->init(&argc, argv));

    // Setting up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path));

    // Get a hostname
    std::string hostname = wrench::Simulation::getHostnameList()[0];

    // Create a Storage Service
    ASSERT_NO_THROW(storage_service = simulation->add(
                            wrench::SimpleStorageService::createSimpleStorageService(hostname, {"/"})));

    // Create a bare-metal compute service
    std::string execution_host = wrench::Simulation::getHostnameList()[1];
    auto baremetal_compute_service = simulation->add(new wrench::BareMetalComputeService(
            execution_host,
            {std::make_pair(
                    execution_host,
                    std::make_tuple(wrench::Simulation::getHostNumCores(execution_host),
                                    wrench::Simulation::getHostMemoryCapacity(execution_host)))},
            "/scratch"));

    // Create a BatchComputeService compute service
    auto batch_compute_service = simulation->add(new wrench::BatchComputeService(execution_host,
                                                                                 {execution_host},
                                                                                 "/scratch2"));

    // Create a HTCondor Service
    std::set<std::shared_ptr<wrench::ComputeService>> compute_services;
    compute_services.insert(baremetal_compute_service);
    compute_services.insert(batch_compute_service);

    ASSERT_NO_THROW(htcondor_service = simulation->add(
                            new wrench::HTCondorComputeService(hostname, compute_services,
                                                               {})));

    // Create a WMS
    std::shared_ptr<wrench::ExecutionController> wms = nullptr;

    ASSERT_NO_THROW(wms = simulation->add(
                            new HTCondorPilotJobTestWMS(this, hostname)));

    // Create a file registry
    ASSERT_NO_THROW(simulation->add(new wrench::FileRegistryService(hostname)));

    // Staging the input_file on the storage service
    ASSERT_NO_THROW(simulation->stageFile(input_file, storage_service));

    // Running a "run a single task1" simulation
    ASSERT_NO_THROW(simulation->launch());


    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}

/**********************************************************************/
/**  SIMPLE SERVICE TESTS                                            **/
/**********************************************************************/

class HTCondorSimpleServiceTestWMS : public wrench::ExecutionController {

public:
    HTCondorSimpleServiceTestWMS(HTCondorServiceTest *test,
                                 std::string &hostname) : wrench::ExecutionController(hostname, "test") {
        this->test = test;
    }

private:
    HTCondorServiceTest *test;

    int main() override {
        // Create a data movement manager
        auto data_movement_manager = this->createDataMovementManager();

        // Create a job manager
        auto job_manager = this->createJobManager();

        auto htcondor_cs = this->test->htcondor_service;
        // Create a job
        auto two_task_job = job_manager->createStandardJob(
                {this->test->task1}, (std::map<std::shared_ptr<wrench::DataFile>, std::shared_ptr<wrench::FileLocation>>){},
                {std::make_tuple(
                        wrench::FileLocation::LOCATION(htcondor_cs->getLocalStorageService(), this->test->input_file),
                        wrench::FileLocation::SCRATCH(this->test->input_file))},
                {}, {});

        // Submit the job for execution
        try {
            job_manager->submitJob(two_task_job, this->test->htcondor_service);
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error(e.what());
        }

        // Wait for a workflow execution event
        std::shared_ptr<wrench::ExecutionEvent> event;
        try {
            event = this->waitForNextEvent();
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Error while getting an execution event: " + e.getCause()->toString());
        }
        if (not std::dynamic_pointer_cast<wrench::StandardJobCompletedEvent>(event)) {
            throw std::runtime_error("Unexpected workflow execution event: " + event->toString());
        }

        this->test->htcondor_service->stop();

        return 0;
    }
};

TEST_F(HTCondorServiceTest, HTCondorSimpleServiceTest) {
    DO_TEST_WITH_FORK(do_SimpleServiceTest_test);
}

void HTCondorServiceTest::do_SimpleServiceTest_test() {

    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    auto argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    //    argv[1] = strdup("--wrench-full-log");

    ASSERT_NO_THROW(simulation->init(&argc, argv));

    // Setting up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path));

    // Get a hostname
    std::string hostname = wrench::Simulation::getHostnameList()[0];

    // Create a Storage Service
    ASSERT_NO_THROW(storage_service = simulation->add(wrench::SimpleStorageService::createSimpleStorageService(hostname, {"/"})));

    {
        // Create list of invalid compute services
        std::set<std::shared_ptr<wrench::ComputeService>> invalid_compute_services;
        std::string execution_host = wrench::Simulation::getHostnameList()[1];
        std::vector<std::string> execution_hosts;
        execution_hosts.push_back(execution_host);
        auto cloud_compute_service = simulation->add(
                new wrench::CloudComputeService(hostname, execution_hosts,
                                                "/scratch"));
        invalid_compute_services.insert(cloud_compute_service);

        ASSERT_THROW(htcondor_service = simulation->add(
                             new wrench::HTCondorComputeService(hostname, std::move(invalid_compute_services),
                                                                {})),
                     std::invalid_argument);
    }


    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}


/**********************************************************************/
/**          STANDARD JOB SUBMISSION TEST FOR GRID UNIVERSE          **/
/**********************************************************************/

class HTCondorGridUniverseTestWMS : public wrench::ExecutionController {

public:
    HTCondorGridUniverseTestWMS(HTCondorServiceTest *test,
                                std::string &hostname) : wrench::ExecutionController(hostname, "test") {
        this->test = test;
    }

private:
    HTCondorServiceTest *test;

    int main() override {
        // Create a data movement manager
        auto data_movement_manager = this->createDataMovementManager();

        // Create a job manager
        auto job_manager = this->createJobManager();


        auto htcondor_cs = this->test->htcondor_service;
        std::map<std::shared_ptr<wrench::DataFile>, std::shared_ptr<wrench::FileLocation>> file_locations;
        file_locations[this->test->input_file2] = wrench::FileLocation::LOCATION(htcondor_cs->getLocalStorageService(), this->test->input_file2);
        std::shared_ptr<wrench::StandardJob> grid_job = job_manager->createStandardJob(
                {this->test->task7},
                file_locations,
                {std::make_tuple(
                        wrench::FileLocation::LOCATION(this->test->storage_service, this->test->input_file2),
                        wrench::FileLocation::LOCATION(htcondor_cs->getLocalStorageService(), this->test->input_file2))},
                {}, {});


        std::map<std::string, std::string> service_specific_arguments;
        service_specific_arguments["-universe"] = "grid";
        service_specific_arguments["-N"] = "1";
        service_specific_arguments["-c"] = "1";
        service_specific_arguments["-t"] = "3600";

        // Submit the job for execution
        try {
            job_manager->submitJob(grid_job, this->test->htcondor_service, service_specific_arguments);
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error(e.what());
        }

        // Wait for a workflow execution event
        std::shared_ptr<wrench::ExecutionEvent> event;
        try {
            event = this->waitForNextEvent();
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Error while getting an execution event: " + e.getCause()->toString());
        }

        if (not std::dynamic_pointer_cast<wrench::StandardJobCompletedEvent>(event)) {
            throw std::runtime_error("Unexpected workflow execution event: " + event->toString());
        }

        this->test->htcondor_service->stop();

        return 0;
    }
};

TEST_F(HTCondorServiceTest, HTCondorGridUniverseTest) {
    DO_TEST_WITH_FORK(do_GridUniverseTest_test);
}

void HTCondorServiceTest::do_GridUniverseTest_test() {

    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    auto argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    //    argv[1] = strdup("--wrench-full-log");

    ASSERT_NO_THROW(simulation->init(&argc, argv));

    // Setting up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path1));

    // Get a hostname
    std::string hostname = "DualCoreHost";
    //std::string batchhostname = "BatchHost1";

    // Create a Storage Service
    ASSERT_NO_THROW(storage_service = simulation->add(
                            wrench::SimpleStorageService::createSimpleStorageService(hostname, {"/"})));

    //ASSERT_NO_THROW(storage_service2 = simulation->add(
    //       wrench::SimpleStorageService::createSimpleStorageService(batchhostname, {"/"})));

    // Create list of compute services
    std::string execution_host = wrench::Simulation::getHostnameList()[1];
    std::vector<std::string> execution_hosts;
    execution_hosts.push_back(execution_host);

    auto batch_service = simulation->add(new wrench::BatchComputeService("BatchHost1",
                                                                         {"BatchHost1", "BatchHost2"},
                                                                         "/scratch",
                                                                         {}));

    std::set<std::shared_ptr<wrench::ComputeService>> compute_services;
    compute_services.insert(batch_service);

    // Create a HTCondor Service with BatchComputeService service
    ASSERT_NO_THROW(htcondor_service = simulation->add(
                            new wrench::HTCondorComputeService(
                                    hostname, std::move(compute_services),
                                    {},
                                    {})));


    htcondor_service->setLocalStorageService(storage_service);

    // Create a WMS
    std::shared_ptr<wrench::ExecutionController> wms = nullptr;

    ASSERT_NO_THROW(wms = simulation->add(
                            new HTCondorGridUniverseTestWMS(this, hostname)));

    // Create a file registry
    ASSERT_NO_THROW(simulation->add(new wrench::FileRegistryService(hostname)));

    // Staging the input_file on the storage service
    ASSERT_NO_THROW(simulation->stageFile(input_file2, storage_service));
    ASSERT_NO_THROW(simulation->stageFile(input_file3, storage_service));

    // Running a "run a single task1" simulation
    ASSERT_NO_THROW(simulation->launch());

    //simulation->getOutput().dumpUnifiedJSON(grid_workflow, "/tmp/workflow_data.json", false, true, false, false, false, false, false);

    for (int i = 0; i < argc; i++) {
        free(argv[i]);
    }
    free(argv);
}


/************************************************************************************************/
/**          GRID UNIVERSE JOB SUBMISSION TEST WHERE GRID UNIVERSE JOBS NOT SUPPORTED          **/
/************************************************************************************************/

class HTCondorNoGridUniverseJobTestWMS : public wrench::ExecutionController {

public:
    HTCondorNoGridUniverseJobTestWMS(HTCondorServiceTest *test,
                                     std::string &hostname) : wrench::ExecutionController(hostname, "test") {
        this->test = test;
    }

private:
    HTCondorServiceTest *test;

    int main() override {
        // Create a data movement manager
        auto data_movement_manager = this->createDataMovementManager();

        // Create a job manager
        auto job_manager = this->createJobManager();


        auto htcondor_cs = this->test->htcondor_service;

        auto grid_job = job_manager->createStandardJob(
                {this->test->task7},
                (std::map<std::shared_ptr<wrench::DataFile>, std::shared_ptr<wrench::FileLocation>>){},
                {std::make_tuple(
                        wrench::FileLocation::LOCATION(htcondor_cs->getLocalStorageService(), this->test->input_file2),
                        wrench::FileLocation::SCRATCH(this->test->input_file2))},
                {}, {});


        std::map<std::string, std::string> test_service_specs;
        test_service_specs["-universe"] = "grid";

        // Submit the  job for execution
        try {
            job_manager->submitJob(grid_job, this->test->htcondor_service, test_service_specs);
            throw std::runtime_error("Shouldn't have been able to submit a grid job");
        } catch (std::invalid_argument &ignore) {
            return 0;
        }


        return 1;
    }
};

TEST_F(HTCondorServiceTest, NoGridUniverseSupportTest) {
    DO_TEST_WITH_FORK(do_NoGridUniverseSupportTest_test);
}

void HTCondorServiceTest::do_NoGridUniverseSupportTest_test() {

    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    auto argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");

    ASSERT_NO_THROW(simulation->init(&argc, argv));

    // Setting up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path1));


    // Get a hostname
    std::string hostname = "DualCoreHost";

    // Create a Storage Service
    ASSERT_NO_THROW(storage_service = simulation->add(
                            wrench::SimpleStorageService::createSimpleStorageService(hostname, {"/"})));

    // Create list of compute services
    std::set<std::shared_ptr<wrench::ComputeService>> compute_services;

    std::string execution_host = wrench::Simulation::getHostnameList()[1];
    std::vector<std::string> execution_hosts;
    execution_hosts.push_back(execution_host);
    compute_services.insert(simulation->add(new wrench::BareMetalComputeService(
            execution_host,
            {std::make_pair(
                    execution_host,
                    std::make_tuple(wrench::Simulation::getHostNumCores(execution_host),
                                    wrench::Simulation::getHostMemoryCapacity(execution_host)))},
            "/scratch")));

    ASSERT_NO_THROW(htcondor_service = simulation->add(
                            new wrench::HTCondorComputeService(
                                    hostname, std::move(compute_services),
                                    {},
                                    {})));

    htcondor_service->setLocalStorageService(storage_service);

    // Create a WMS
    std::shared_ptr<wrench::ExecutionController> wms = nullptr;

    ASSERT_NO_THROW(wms = simulation->add(
                            new HTCondorNoGridUniverseJobTestWMS(this, hostname)));

    // Create a file registry
    ASSERT_NO_THROW(simulation->add(new wrench::FileRegistryService(hostname)));

    // Staging the input_file on the storage service
    ASSERT_NO_THROW(simulation->stageFile(input_file2, storage_service));
    ASSERT_NO_THROW(simulation->stageFile(input_file3, storage_service));

    // Fails because it will try to run a grid job on a system that does not support standard jobs, WMS is set up to
    //return 0 in this case and 1 if it successfully completes.
    ASSERT_NO_THROW(simulation->launch());


    for (int i = 0; i < argc; i++) {
        free(argv[i]);
    }
    free(argv);
}


/************************************************************************************************/
/**          GRID UNIVERSE JOB SUBMISSION TEST WHERE NON-GRID UNIVERSE JOBS NOT SUPPORTED          **/
/************************************************************************************************/

class HTCondorNoNonGridUniverseJobTestWMS : public wrench::ExecutionController {

public:
    HTCondorNoNonGridUniverseJobTestWMS(HTCondorServiceTest *test,
                                        std::string &hostname) : wrench::ExecutionController(hostname, "test") {
        this->test = test;
    }

private:
    HTCondorServiceTest *test;

    int main() override {
        // Create a data movement manager
        auto data_movement_manager = this->createDataMovementManager();

        // Create a job manager
        auto job_manager = this->createJobManager();


        auto htcondor_cs = this->test->htcondor_service;

        auto non_grid_job = job_manager->createStandardJob(
                {this->test->task7},
                (std::map<std::shared_ptr<wrench::DataFile>, std::shared_ptr<wrench::FileLocation>>){},
                {std::make_tuple(
                        wrench::FileLocation::LOCATION(htcondor_cs->getLocalStorageService(), this->test->input_file2),
                        wrench::FileLocation::SCRATCH(this->test->input_file2))},
                {}, {});


        std::map<std::string, std::string> test_service_specs;// empty, non-grid

        // Submit the  job for execution
        try {
            job_manager->submitJob(non_grid_job, this->test->htcondor_service, test_service_specs);
            //            throw std::runtime_error("Shouldn't have been able to submit a grid job");
            //            return 1;
        } catch (std::invalid_argument &ignore) {
            //            return 0;
        }

        std::shared_ptr<wrench::ExecutionEvent> event;
        try {
            event = this->waitForNextEvent();
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Error while getting an execution event: " + e.getCause()->toString());
        }
        if (not std::dynamic_pointer_cast<wrench::StandardJobFailedEvent>(event)) {
            throw std::runtime_error("Unexpected event: " + event->toString());
        }
        auto real_event = std::dynamic_pointer_cast<wrench::StandardJobFailedEvent>(event);
        if (not std::dynamic_pointer_cast<wrench::NotAllowed>(real_event->failure_cause)) {
            throw std::runtime_error("Unexpected failure cause: " + real_event->failure_cause->toString());
        }

        return 0;
    }
};

TEST_F(HTCondorServiceTest, NoNonGridUniverseSupportTest) {
    DO_TEST_WITH_FORK(do_NoNonGridUniverseSupportTest_test);
}

void HTCondorServiceTest::do_NoNonGridUniverseSupportTest_test() {

    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    auto argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    //    argv[1] = strdup("--wrench-full-log");

    ASSERT_NO_THROW(simulation->init(&argc, argv));

    // Setting up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path1));


    // Get a hostname
    std::string hostname = "DualCoreHost";

    // Create a Storage Service
    ASSERT_NO_THROW(storage_service = simulation->add(
                            wrench::SimpleStorageService::createSimpleStorageService(hostname, {"/"})));

    // Create list of compute services
    std::set<std::shared_ptr<wrench::ComputeService>> compute_services;

    auto batch_service = simulation->add(
            new wrench::BatchComputeService(
                    "BatchHost1",
                    {"BatchHost1", "BatchHost2"},
                    "/scratch",
                    {}));
    compute_services.insert(batch_service);

    ASSERT_NO_THROW(htcondor_service = simulation->add(
                            new wrench::HTCondorComputeService(
                                    hostname, std::move(compute_services),
                                    {},
                                    {})));


    htcondor_service->setLocalStorageService(storage_service);

    // Create a WMS
    std::shared_ptr<wrench::ExecutionController> wms = nullptr;

    ASSERT_NO_THROW(wms = simulation->add(
                            new HTCondorNoNonGridUniverseJobTestWMS(this, hostname)));

    // Create a file registry
    ASSERT_NO_THROW(simulation->add(new wrench::FileRegistryService(hostname)));

    // Staging the input_file on the storage service
    ASSERT_NO_THROW(simulation->stageFile(input_file2, storage_service));
    ASSERT_NO_THROW(simulation->stageFile(input_file3, storage_service));

    // Fails because it will try to run a grid job on a system that does not support standard jobs, WMS is set up to
    //return 0 in this case and 1 if it successfully completes.
    ASSERT_NO_THROW(simulation->launch());


    for (int i = 0; i < argc; i++) {
        free(argv[i]);
    }
    free(argv);
}


/************************************************************************************************/
/**          PILOT JOB SUBMISSION TEST WHERE GRID  JOBS NOT SUPPORTED                          **/
/************************************************************************************************/

class HTCondorNoGridJobTestWMS : public wrench::ExecutionController {

public:
    HTCondorNoGridJobTestWMS(HTCondorServiceTest *test,
                             std::string &hostname) : wrench::ExecutionController(hostname, "test") {
        this->test = test;
    }

private:
    HTCondorServiceTest *test;

    int main() override {
        // Create a data movement manager
        auto data_movement_manager = this->createDataMovementManager();

        // Create a job manager
        auto job_manager = this->createJobManager();


        auto htcondor_cs = this->test->htcondor_service;
        std::shared_ptr<wrench::PilotJob> grid_job = job_manager->createPilotJob();


        std::map<std::string, std::string> test_service_specs;
        test_service_specs["-universe"] = "grid";
        test_service_specs["-N"] = "1";
        test_service_specs["-c"] = "1";
        test_service_specs["-t"] = "1";

        // Submit the job for execution
        try {
            job_manager->submitJob(grid_job, this->test->htcondor_service, test_service_specs);
            throw std::runtime_error("Should not have been able to submit job successfully");
        } catch (std::invalid_argument &ignore) {
            return 0;
        }
    }
};

TEST_F(HTCondorServiceTest, NoGridJobSupportTest) {
    DO_TEST_WITH_FORK(do_NoGridJobSupportTest_test);
}

void HTCondorServiceTest::do_NoGridJobSupportTest_test() {

    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    auto argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");

    ASSERT_NO_THROW(simulation->init(&argc, argv));

    // Setting up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path1));

    // Get a hostname
    std::string hostname = "DualCoreHost";

    // Create a Storage Service
    ASSERT_NO_THROW(storage_service = simulation->add(
                            wrench::SimpleStorageService::createSimpleStorageService(hostname, {"/"})));

    // Create list of compute services
    std::set<std::shared_ptr<wrench::ComputeService>> compute_services;

    std::string execution_host = wrench::Simulation::getHostnameList()[1];
    std::vector<std::string> execution_hosts;
    execution_hosts.push_back(execution_host);
    compute_services.insert(simulation->add(new wrench::BareMetalComputeService(
            execution_host,
            {std::make_pair(
                    execution_host,
                    std::make_tuple(wrench::Simulation::getHostNumCores(execution_host),
                                    wrench::Simulation::getHostMemoryCapacity(execution_host)))},
            "/scratch")));

    ASSERT_NO_THROW(htcondor_service = simulation->add(
                            new wrench::HTCondorComputeService(
                                    hostname, std::move(compute_services),
                                    {},
                                    {})));


    htcondor_service->setLocalStorageService(storage_service);

    // Create a WMS
    std::shared_ptr<wrench::ExecutionController> wms = nullptr;

    ASSERT_NO_THROW(wms = simulation->add(
                            new HTCondorNoGridJobTestWMS(this, hostname)));

    // Create a file registry
    ASSERT_NO_THROW(simulation->add(new wrench::FileRegistryService(hostname)));

    // Staging the input_file on the storage service
    ASSERT_NO_THROW(simulation->stageFile(input_file2, storage_service));
    ASSERT_NO_THROW(simulation->stageFile(input_file3, storage_service));

    // Fails because it will try to run a grid job on a system that does not support standard jobs, WMS is set up to
    //return 0 in this case and 1 if it successfully completes.
    ASSERT_NO_THROW(simulation->launch());


    for (int i = 0; i < argc; i++) {
        free(argv[i]);
    }
    free(argv);
}


/************************************************************************************************/
/**          STANDARD JOB SUBMISSION TEST WHERE NOT ENOUGH RESOURCES                          **/
/************************************************************************************************/

class HTCondorNotEnoughResourcesTestWMS : public wrench::ExecutionController {

public:
    HTCondorNotEnoughResourcesTestWMS(HTCondorServiceTest *test,
                                      std::string &hostname) : wrench::ExecutionController(hostname, "test") {
        this->test = test;
    }

private:
    HTCondorServiceTest *test;

    int main() override {
        // Create a data movement manager
        auto data_movement_manager = this->createDataMovementManager();

        // Create a job manager
        auto job_manager = this->createJobManager();

        auto htcondor_cs = this->test->htcondor_service;

        {
            // Submit a grid universe job that asks for too much
            std::map<std::shared_ptr<wrench::DataFile>, std::shared_ptr<wrench::FileLocation>> file_locations;
            file_locations[this->test->input_file] = wrench::FileLocation::LOCATION(this->test->storage_service, this->test->input_file);
            file_locations[this->test->output_file1] = wrench::FileLocation::LOCATION(this->test->storage_service, this->test->output_file1);
            auto grid_job = job_manager->createStandardJob(
                    {this->test->task1},
                    file_locations,
                    {},
                    {}, {});

            std::map<std::string, std::string> test_service_specs;
            test_service_specs["-universe"] = "grid";
            test_service_specs["-N"] = "100";
            test_service_specs["-c"] = "1";
            test_service_specs["-t"] = "1";

            // Submit the job for execution
            try {
                job_manager->submitJob(grid_job, this->test->htcondor_service, test_service_specs);
                throw std::runtime_error("Should not have been able to submit job successfully");
            } catch (wrench::ExecutionException &e) {
                auto real_cause = std::dynamic_pointer_cast<wrench::NotEnoughResources>(e.getCause());
                if (not real_cause) {
                    throw std::runtime_error("Should have gotten a NotEnoughResources failure cause");
                }
            }
        }

        {
            auto big_task = this->test->workflow->addTask("big_task", 1000.0, 100, 100, 0);
            auto ngrid_job = job_manager->createStandardJob(big_task);
            // Submit the job for execution
            try {
                job_manager->submitJob(ngrid_job, this->test->htcondor_service, {});
                //                throw std::runtime_error("Should not have been able to submit job successfully");
            } catch (wrench::ExecutionException &e) {
                auto real_cause = std::dynamic_pointer_cast<wrench::NotEnoughResources>(e.getCause());
                if (not real_cause) {
                    throw std::runtime_error("Should have gotten a NotEnoughResources failure cause");
                }
            }

            // Wait for next event
            std::shared_ptr<wrench::ExecutionEvent> event;
            try {
                event = this->waitForNextEvent();
            } catch (wrench::ExecutionException &e) {
                throw std::runtime_error("Error while getting an execution event: " + e.getCause()->toString());
            }
            auto real_event = std::dynamic_pointer_cast<wrench::StandardJobFailedEvent>(event);
            if (not real_event) {
                throw std::runtime_error("Unexpected workflow execution event: " + event->toString());
            }
            if (not std::dynamic_pointer_cast<wrench::NotEnoughResources>(real_event->failure_cause)) {
                throw std::runtime_error("Unexpected failure cause: " + real_event->failure_cause->toString());
            }
        }
        return 0;
    }
};

TEST_F(HTCondorServiceTest, NotEnoughResourcesTest) {
    DO_TEST_WITH_FORK(do_NotEnoughResourcesTest_test);
}

void HTCondorServiceTest::do_NotEnoughResourcesTest_test() {

    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    auto argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");

    ASSERT_NO_THROW(simulation->init(&argc, argv));

    // Setting up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path1));

    // Get a hostname
    std::string hostname = "DualCoreHost";

    // Create a Storage Service
    ASSERT_NO_THROW(storage_service = simulation->add(
                            wrench::SimpleStorageService::createSimpleStorageService(hostname, {"/"})));

    // Create list of compute services
    std::set<std::shared_ptr<wrench::ComputeService>> compute_services;

    std::string execution_host = wrench::Simulation::getHostnameList()[1];
    std::vector<std::string> execution_hosts;
    execution_hosts.push_back(execution_host);
    compute_services.insert(simulation->add(new wrench::BareMetalComputeService(
            execution_host,
            {std::make_pair(
                    execution_host,
                    std::make_tuple(wrench::Simulation::getHostNumCores(execution_host),
                                    wrench::Simulation::getHostMemoryCapacity(execution_host)))},
            "/scratch")));

    auto batch_service = simulation->add(new wrench::BatchComputeService("BatchHost1",
                                                                         {"BatchHost1", "BatchHost2"},
                                                                         "/scratch",
                                                                         {}));
    compute_services.insert(batch_service);

    ASSERT_NO_THROW(htcondor_service = simulation->add(
                            new wrench::HTCondorComputeService(
                                    hostname, std::move(compute_services),
                                    {},
                                    {})));

    htcondor_service->setLocalStorageService(storage_service);

    // Create a WMS
    std::shared_ptr<wrench::ExecutionController> wms = nullptr;

    ASSERT_NO_THROW(wms = simulation->add(
                            new HTCondorNotEnoughResourcesTestWMS(this, hostname)));

    // Create a file registry
    ASSERT_NO_THROW(simulation->add(new wrench::FileRegistryService(hostname)));

    // Staging the input_file on the storage service
    ASSERT_NO_THROW(simulation->stageFile(input_file2, storage_service));
    ASSERT_NO_THROW(simulation->stageFile(input_file3, storage_service));

    // Fails because it will try to run a grid job on a system that does not support standard jobs, WMS is set up to
    //return 0 in this case and 1 if it successfully completes.
    ASSERT_NO_THROW(simulation->launch());


    for (int i = 0; i < argc; i++) {
        free(argv[i]);
    }
    free(argv);
}


/************************************************************************************************/
/**          STANDARD JOB SUBMISSION TEST WITH USE OF SCRATCH                                  **/
/************************************************************************************************/

class HTCondorScratchTestWMS : public wrench::ExecutionController {

public:
    HTCondorScratchTestWMS(HTCondorServiceTest *test,
                           std::string &hostname) : wrench::ExecutionController(hostname, "test") {
        this->test = test;
    }

private:
    HTCondorServiceTest *test;

    int main() override {

        // Create a job manager
        auto job_manager = this->createJobManager();

        auto htcondor_cs = this->test->htcondor_service;

        // Create an input file
        this->test->storage_service->createFile(this->test->input_file);

        // Create a job that uses scratch
        std::map<std::shared_ptr<wrench::DataFile>, std::shared_ptr<wrench::FileLocation>> file_locations;
        file_locations[this->test->input_file] = wrench::FileLocation::SCRATCH(this->test->input_file);
        std::vector<std::tuple<std::shared_ptr<wrench::FileLocation>, std::shared_ptr<wrench::FileLocation>>> pre_file_copies;
        pre_file_copies.emplace_back(
                wrench::FileLocation::LOCATION(this->test->storage_service, this->test->input_file),
                wrench::FileLocation::SCRATCH(this->test->input_file));
        std::vector<std::tuple<std::shared_ptr<wrench::FileLocation>, std::shared_ptr<wrench::FileLocation>>> post_file_copies;
        post_file_copies.emplace_back(
                wrench::FileLocation::SCRATCH(this->test->output_file1),
                wrench::FileLocation::LOCATION(this->test->storage_service, this->test->output_file1));


        auto job = job_manager->createStandardJob({this->test->task1}, file_locations,
                                                  pre_file_copies, post_file_copies, {});


        // Submit the job for execution
        try {
            job_manager->submitJob(job, this->test->htcondor_service);
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Should have been able to submit the job");
        }


        // Wait for next event
        std::shared_ptr<wrench::ExecutionEvent> event;
        try {
            event = this->waitForNextEvent();
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Error while getting an execution event: " + e.getCause()->toString());
        }
        auto real_event = std::dynamic_pointer_cast<wrench::StandardJobCompletedEvent>(event);
        if (not real_event) {
            throw std::runtime_error("Unexpected workflow execution event: " + event->toString());
        }

        return 0;
    }
};

TEST_F(HTCondorServiceTest, ScratchTest) {
    DO_TEST_WITH_FORK(do_ScratchTest_test);
}

void HTCondorServiceTest::do_ScratchTest_test() {

    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    auto argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    //        argv[1] = strdup("--wrench-full-log");

    ASSERT_NO_THROW(simulation->init(&argc, argv));

    // Setting up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path1));

    // Get a hostname
    std::string hostname = "DualCoreHost";

    // Create a Storage Service
    ASSERT_NO_THROW(storage_service = simulation->add(
                            wrench::SimpleStorageService::createSimpleStorageService(hostname, {"/"})));

    // Create list of one bare-metal compute service
    std::set<std::shared_ptr<wrench::ComputeService>> compute_services;

    std::string execution_host = wrench::Simulation::getHostnameList()[1];
    std::vector<std::string> execution_hosts;
    execution_hosts.push_back(execution_host);
    compute_services.insert(simulation->add(new wrench::BareMetalComputeService(
            execution_host,
            {std::make_pair(
                    execution_host,
                    std::make_tuple(wrench::Simulation::getHostNumCores(execution_host),
                                    wrench::Simulation::getHostMemoryCapacity(execution_host)))},
            "/scratch")));

    ASSERT_NO_THROW(htcondor_service = simulation->add(
                            new wrench::HTCondorComputeService(
                                    hostname, std::move(compute_services),
                                    {},
                                    {})));

    htcondor_service->setLocalStorageService(storage_service);

    // Create a WMS
    std::shared_ptr<wrench::ExecutionController> wms = nullptr;

    ASSERT_NO_THROW(wms = simulation->add(
                            new HTCondorScratchTestWMS(this, hostname)));

    ASSERT_NO_THROW(simulation->launch());

    for (int i = 0; i < argc; i++) {
        free(argv[i]);
    }
    free(argv);
}