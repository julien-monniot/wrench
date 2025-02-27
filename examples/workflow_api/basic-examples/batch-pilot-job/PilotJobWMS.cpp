/**
 * Copyright (c) 2017-2021. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

/**
 ** A Workflow Management System (WMS) implementation that operates as follows:
 **
 **  - Creates a pilot job, but not long enough to accommodate both tasks
 **  - Submit the first task to the pilot job  as a standard job
 **  - Intermediate file is kept in the batch_standard_and_pilot_jobs compute service's scratch space!
 **  - Submit the second task to the pilot job  as a standard job
 **  - The pilot job will expire because the second task completes, and the
 **    WMS gives up
 **/

#include <iostream>

#include "PilotJobWMS.h"

WRENCH_LOG_CATEGORY(custom_wms, "Log category for PilotJobWMS");

namespace wrench {

    /**
     * @brief Constructor, which calls the super constructor
     *
     * @param workflow: the workflow to execute
     * @param batch_compute_service: a batch compute services available to run tasks
     * @param storage_service: a storage service available to store files
     * @param hostname: the name of the host on which to start the WMS
     */
    PilotJobWMS::PilotJobWMS(std::shared_ptr<Workflow> workflow,
                             const std::shared_ptr<BatchComputeService> &batch_compute_service,
                             const std::shared_ptr<StorageService> &storage_service,
                             const std::string &hostname) : ExecutionController(hostname, "two-tasks-at-a-time-batch_standard_and_pilot_jobs"),
                                                            workflow(workflow), batch_compute_service(batch_compute_service), storage_service(storage_service) {}

    /**
     * @brief main method of the PilotJobWMS daemon
     *
     * @return 0 on completion
     *
     * @throw std::runtime_error
     */
    int PilotJobWMS::main() {
        /* Set the logging output to GREEN */
        TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_GREEN);

        WRENCH_INFO("WMS starting on host %s", Simulation::getHostName().c_str());
        WRENCH_INFO(
                "About to execute a workflow with %lu tasks", this->workflow->getNumberOfTasks());

        /* Create a job manager so that we can create/submit jobs */
        auto job_manager = this->createJobManager();


        /* Record the batch_standard_and_pilot_jobs node's core flop rate */
        double core_flop_rate = (*(batch_compute_service->getCoreFlopRate().begin())).second;

        /*  Get references to tasks and files */
        auto task_0 = this->workflow->getTaskByID("task_0");
        auto task_1 = this->workflow->getTaskByID("task_1");
        auto file_0 = this->workflow->getFileByID("file_0");
        auto file_1 = this->workflow->getFileByID("file_1");
        auto file_2 = this->workflow->getFileByID("file_2");

        /* For each task, estimate its execution time in seconds */
        std::map<std::shared_ptr<WorkflowTask>, long> execution_times_in_seconds;
        for (auto const &t: this->workflow->getTasks()) {
            double parallel_efficiency =
                    std::dynamic_pointer_cast<wrench::ConstantEfficiencyParallelModel>(t->getParallelModel())->getEfficiency();
            double in_seconds = (t->getFlops() / core_flop_rate) / (10 * parallel_efficiency);
            execution_times_in_seconds[t] = 1 + std::lround(in_seconds);
            // The +1 above is just  so that we don't cut it too tight
            WRENCH_INFO("Task %s should run in under %ld seconds",
                        t->getID().c_str(), execution_times_in_seconds[t]);
        }

        /* Create a Pilot job */
        auto pilot_job = job_manager->createPilotJob();

        std::map<std::string, std::string> service_specific_arguments;
        // number of nodes
        service_specific_arguments["-N"] = "2";
        // number of cores
        service_specific_arguments["-c"] = "10";
        // time: not enough to run both tasks
        service_specific_arguments["-t"] =
                std::to_string((execution_times_in_seconds[task_0] + execution_times_in_seconds[task_1]) / 2);

        WRENCH_INFO("Submitting a pilot job that  requests %s %s-core nodes for %s seconds",
                    service_specific_arguments["-N"].c_str(),
                    service_specific_arguments["-c"].c_str(),
                    service_specific_arguments["-t"].c_str());

        job_manager->submitJob(pilot_job, batch_compute_service, service_specific_arguments);

        /*  Waiting for the pilot job to start */
        WRENCH_INFO("Waiting and event");
        this->waitForAndProcessNextEvent();

        /* At this point, we know (in this simple example) the pilot job has started */
        /* Submit a job that runs both tasks to the pilot job  */
        WRENCH_INFO("Creating a standard job for both tasks");
        auto cs = pilot_job->getComputeService();

        /* Create a map of file locations, stating for each file
            * where it should be read/written */
        std::map<std::shared_ptr<DataFile>, std::shared_ptr<FileLocation>> file_locations;
        file_locations[file_0] = FileLocation::LOCATION(storage_service, file_0);
        file_locations[file_1] = FileLocation::LOCATION(storage_service, file_1);
        file_locations[file_2] = FileLocation::LOCATION(storage_service, file_2);

        auto standard_job = job_manager->createStandardJob({task_0, task_1}, file_locations);

        WRENCH_INFO("Submitting the standard job to the pilot job");
        job_manager->submitJob(standard_job, cs);

        WRENCH_INFO("Wait for an event");
        this->waitForAndProcessNextEvent();
        WRENCH_INFO("Wait for an event");
        this->waitForAndProcessNextEvent();

        return 0;
    }

    /**
     * @brief Process a standard job completion event
     *
     * @param event: the event
     */
    void PilotJobWMS::processEventStandardJobCompletion(std::shared_ptr<StandardJobCompletedEvent> event) {
        WRENCH_INFO("Notified that a standard job has completed");
        throw std::runtime_error("This shouldn't happen in this example");
    }

    /**
     * @brief Process a standard job failure event
     *
     * @param event: the event
     */
    void PilotJobWMS::processEventStandardJobFailure(std::shared_ptr<StandardJobFailedEvent> event) {
        /* Retrieve the job that this event is for */
        auto job = event->standard_job;
        /* Retrieve the job's first (and in our case only) task */
        WRENCH_INFO("Notified that a standard job has failed due to: %s ",
                    event->failure_cause->toString().c_str());
    }

    /**
     * @brief Process a pilot job expiration event
     *
     * @param event: the event
     */
    void PilotJobWMS::processEventPilotJobExpiration(std::shared_ptr<PilotJobExpiredEvent>) {
        WRENCH_INFO("Notified that a pilot job has expired");
    }

    /**
     * @brief Process a pilot job start event
     *
     * @param event: the event
     */
    void PilotJobWMS::processEventPilotJobStart(std::shared_ptr<PilotJobStartedEvent>) {
        WRENCH_INFO("Notified that a pilot job has started!");
    }


}// namespace wrench
