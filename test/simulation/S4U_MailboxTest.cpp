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
#include "../include/UniqueTmpPathPrefix.h"
#include "../include/TestWithFork.h"

WRENCH_LOG_CATEGORY(s4u_mailbox_test, "Log category for S4U_Mailbo test");


class S4U_MailboxTest : public ::testing::Test {

public:
    std::shared_ptr<wrench::ExecutionController> wms1, wms2;

    void do_AsynchronousCommunication_test();
    void do_NetworkTimeout_test();
    void do_NullMailbox_test();

protected:
    S4U_MailboxTest() {


        // Create a two-host platform file
        std::string xml = "<?xml version='1.0'?>"
                          "<!DOCTYPE platform SYSTEM \"https://simgrid.org/simgrid.dtd\">"
                          "<platform version=\"4.1\"> "
                          "   <zone id=\"AS0\" routing=\"Full\"> "
                          "       <host id=\"Host1\" speed=\"1f\" core=\"1\"/> "
                          "       <host id=\"Host2\" speed=\"1f\" core=\"1\"/> "
                          "       <link id=\"1\" bandwidth=\"1Bps\" latency=\"0us\"/>"
                          "       <route src=\"Host1\" dst=\"Host2\"> <link_ctn id=\"1\"/> </route>"
                          "   </zone> "
                          "</platform>";
        FILE *platform_file = fopen(platform_file_path.c_str(), "w");
        fprintf(platform_file, "%s", xml.c_str());
        fclose(platform_file);
    }

    std::string platform_file_path = UNIQUE_TMP_PATH_PREFIX + "platform.xml";
};

/**********************************************************************/
/**  ASYNCHRONOUS COMMUNICATION TEST                                 **/
/**********************************************************************/

class AsynchronousCommunicationTestWMS : public wrench::ExecutionController {

public:
    AsynchronousCommunicationTestWMS(S4U_MailboxTest *test,
                                     const std::string &hostname) : wrench::ExecutionController(hostname, "test") {
        this->test = test;
    }


private:
    S4U_MailboxTest *test;
    std::string mode;

    int main() override {

        unsigned long index;
        if (this == this->test->wms1.get()) {
            /** SENDER **/

            // Empty set of pending comms
            std::vector<wrench::S4U_PendingCommunication *> empty_pending_comms;
            try {
                wrench::S4U_PendingCommunication::waitForSomethingToHappen(empty_pending_comms, -1);
                throw std::runtime_error("Was expecting a std::invalid_argument exception");
            } catch (std::invalid_argument &) {
            }

            // One send
            auto pending_send = wrench::S4U_Mailbox::iputMessage(this->test->wms2->mailbox, new wrench::SimulationMessage(100));
            pending_send->wait();

            // Another send
            auto another_pending_send = wrench::S4U_Mailbox::iputMessage(this->test->wms2->mailbox, new wrench::SimulationMessage(100));
            another_pending_send->wait(0.01 - wrench::Simulation::getCurrentSimulatedDate());

            // Two sends, no timeout
            std::vector<std::shared_ptr<wrench::S4U_PendingCommunication>> sends;
            sends.push_back(wrench::S4U_Mailbox::iputMessage(this->test->wms2->mailbox, new wrench::SimulationMessage(100)));
            sends.push_back(wrench::S4U_Mailbox::iputMessage(this->test->wms2->mailbox, new wrench::SimulationMessage(100)));
            index = wrench::S4U_PendingCommunication::waitForSomethingToHappen(sends, -1);
            sends.at(index)->wait();
            index = wrench::S4U_PendingCommunication::waitForSomethingToHappen(sends, -1);
            sends.at(index)->wait();

            // Two sends, timeout
            std::vector<std::shared_ptr<wrench::S4U_PendingCommunication>> sends_timeout;
            sends_timeout.push_back(wrench::S4U_Mailbox::iputMessage(this->test->wms2->mailbox, new wrench::SimulationMessage(100)));
            sends_timeout.push_back(wrench::S4U_Mailbox::iputMessage(this->test->wms2->mailbox, new wrench::SimulationMessage(100)));
            double now = wrench::Simulation::getCurrentSimulatedDate();
            index = wrench::S4U_PendingCommunication::waitForSomethingToHappen(sends_timeout, 10);
            if (index != ULONG_MAX) {
                throw std::runtime_error("Was expecting a timeout on the waitForSomethingToHappen");
            }
            if (fabs(10 - (wrench::Simulation::getCurrentSimulatedDate() - now)) > 0.5) {
                throw std::runtime_error("Seems like we didn't wait for the timeout!");
            }
            index = wrench::S4U_PendingCommunication::waitForSomethingToHappen(sends_timeout, 1000);
            sends_timeout.at(index)->wait();
            index = wrench::S4U_PendingCommunication::waitForSomethingToHappen(sends_timeout, 1000);
            sends_timeout.at(index)->wait();

            // One send, network failure
            pending_send = wrench::S4U_Mailbox::iputMessage(this->test->wms2->mailbox, new wrench::SimulationMessage(100));
            wrench::Simulation::sleep(10);
            wrench::Simulation::turnOffLink("1");
            try {
                pending_send->wait();
                throw std::runtime_error("Should have gotten a NetworkError");
            } catch (wrench::ExecutionException &e) {
                auto cause = std::dynamic_pointer_cast<wrench::NetworkError>(e.getCause());
                cause->toString();
                cause->getMailbox();
            }
            wrench::Simulation::sleep(10);
            wrench::Simulation::turnOnLink("1");
            wrench::Simulation::sleep(10);

            //            WRENCH_INFO("TWO ASYNCHRONOUS SENDS / NETWORK FAILURES");

            // Two asynchronous sends, network failure
            std::vector<std::shared_ptr<wrench::S4U_PendingCommunication>> sends_failure;
            sends_failure.push_back(wrench::S4U_Mailbox::iputMessage(this->test->wms2->mailbox, new wrench::SimulationMessage(100)));
            sends_failure.push_back(wrench::S4U_Mailbox::iputMessage(this->test->wms2->mailbox, new wrench::SimulationMessage(100)));
            wrench::Simulation::sleep(10);
            simgrid::s4u::Link::by_name("1")->turn_off();
            //            WRENCH_INFO("SIZE= %ld", sends_failure.size());
            index = wrench::S4U_PendingCommunication::waitForSomethingToHappen(sends_failure, 10000);
            //            WRENCH_INFO("index = %ld", index);
            try {
                sends_failure.at(index)->wait();
                throw std::runtime_error("Should have gotten a NetworkError");
            } catch (wrench::ExecutionException &e) {
                auto cause = std::dynamic_pointer_cast<wrench::NetworkError>(e.getCause());
                cause->toString();
                cause->getMailbox();
            }
            index = wrench::S4U_PendingCommunication::waitForSomethingToHappen(sends_failure, 10000);
            try {
                sends_failure.at(index)->wait();
                throw std::runtime_error("Should have gotten a NetworkError");
            } catch (wrench::ExecutionException &e) {
                auto cause = std::dynamic_pointer_cast<wrench::NetworkError>(e.getCause());
                cause->toString();
                cause->getMailbox();
            }


            // One synchronous sends, network failure
            try {
                wrench::S4U_Mailbox::putMessage(this->test->wms2->mailbox,
                                                new wrench::SimulationMessage(100));
                throw std::runtime_error("Should have gotten a NetworkError");
            } catch (wrench::ExecutionException &e) {
                auto cause = std::dynamic_pointer_cast<wrench::NetworkError>(e.getCause());
                cause->toString();
                cause->getMailbox();
            }


        } else {
            /** RECEIVER **/

            // One recv
            auto pending_recv = wrench::S4U_Mailbox::igetMessage(this->test->wms2->mailbox);
            pending_recv->wait();

            // Another recv
            auto another_pending_recv = wrench::S4U_Mailbox::igetMessage(this->test->wms2->mailbox);
            another_pending_recv->wait(0.01 - wrench::Simulation::getCurrentSimulatedDate());

            // Two recv, no timeout
            std::vector<std::shared_ptr<wrench::S4U_PendingCommunication>> recvs;
            recvs.push_back(wrench::S4U_Mailbox::igetMessage(this->test->wms2->mailbox));
            recvs.push_back(wrench::S4U_Mailbox::igetMessage(this->test->wms2->mailbox));
            index = wrench::S4U_PendingCommunication::waitForSomethingToHappen(recvs, -1);
            recvs.at(index)->wait();
            index = wrench::S4U_PendingCommunication::waitForSomethingToHappen(recvs, -1);
            recvs.at(index)->wait();

            // Two recvs (sends are timing out)
            wrench::S4U_Mailbox::getMessage(this->test->wms2->mailbox);
            wrench::S4U_Mailbox::getMessage(this->test->wms2->mailbox);

            // One recv (which fails)
            try {
                wrench::S4U_Mailbox::getMessage(this->test->wms2->mailbox);
                throw std::runtime_error("Should have gotten a NetworkError");
            } catch (wrench::ExecutionException &e) {
                auto cause = std::dynamic_pointer_cast<wrench::NetworkError>(e.getCause());
                cause->toString();
                cause->getMailbox();
            }

            //            WRENCH_INFO("TWO ASYNCHRPNOUS RECV / NETWORK FAILURES");

            // Two synchronous recv, network failure
            try {
                wrench::S4U_Mailbox::getMessage(this->test->wms2->mailbox);
                throw std::runtime_error("Should have gotten a NetworkError");
            } catch (wrench::ExecutionException &e) {
            }
            try {
                wrench::S4U_Mailbox::getMessage(this->test->wms2->mailbox);
                throw std::runtime_error("Should have gotten a NetworkError");
            } catch (wrench::ExecutionException &e) {
                auto cause = std::dynamic_pointer_cast<wrench::NetworkError>(e.getCause());
                cause->toString();
                cause->getMailbox();
            }

            // One asynchronous recv, network failure
            pending_recv = wrench::S4U_Mailbox::igetMessage(this->test->wms2->mailbox);
            wrench::Simulation::sleep(10);
            try {
                pending_recv->wait();
                throw std::runtime_error("Should have gotten a NetworkError");
            } catch (wrench::ExecutionException &e) {
                auto cause = std::dynamic_pointer_cast<wrench::NetworkError>(e.getCause());
                cause->toString();
                cause->getMailbox();
            }
        }

        return 0;
    }
};

TEST_F(S4U_MailboxTest, AsynchronousCommunication) {
    DO_TEST_WITH_FORK(do_AsynchronousCommunication_test);
}

void S4U_MailboxTest::do_AsynchronousCommunication_test() {


    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();

    int argc = 2;
    auto argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    argv[1] = strdup("--wrench-link-shutdown-simulation");
    //    argv[2] = strdup("--wrench-log-full");

    simulation->init(&argc, argv);

    // Setting up the platform
    simulation->instantiatePlatform(platform_file_path);

    // Create the WMSs
    auto workflow = wrench::Workflow::createWorkflow();
    this->wms1 = simulation->add(new AsynchronousCommunicationTestWMS(this, "Host1"));
    this->wms2 = simulation->add(new AsynchronousCommunicationTestWMS(this, "Host2"));

    simulation->launch();

    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}


/**********************************************************************/
/**  NETWORK TIMEOUT TEST                                            **/
/**********************************************************************/

class NetworkTimeoutTestWMS : public wrench::ExecutionController {

public:
    NetworkTimeoutTestWMS(S4U_MailboxTest *test,
                          const std::string &hostname) : wrench::ExecutionController(hostname, "test") {
        this->test = test;
    }


private:
    S4U_MailboxTest *test;
    std::string mode;

    int main() override {

        try {
            wrench::S4U_Mailbox::getMessage<wrench::SimulationMessage>(this->mailbox, 10);
            throw std::runtime_error("Should have gotten an exception");
        } catch (wrench::ExecutionException &e) {
            auto real_error = std::dynamic_pointer_cast<wrench::NetworkError>(e.getCause());
            if (not real_error) {
                throw std::runtime_error("Unexpected failure cause: " + e.getCause()->toString());
            }
            if (!real_error->isTimeout()) {
                throw std::runtime_error("Network error failure cause should be a time out");
            }
            real_error->toString();// coverage
        }

        try {
            auto pending = wrench::S4U_Mailbox::igetMessage(this->mailbox);
            pending->wait(10);
            throw std::runtime_error("Should have gotten an exception");
        } catch (wrench::ExecutionException &e) {
            auto real_error = std::dynamic_pointer_cast<wrench::NetworkError>(e.getCause());
            if (not real_error) {
                throw std::runtime_error("Unexpected failure cause: " + e.getCause()->toString());
            }
            if (!real_error->isTimeout()) {
                throw std::runtime_error("Network error failure cause should be a time out");
            }
            real_error->toString();// coverage
        }

        {
            auto pending = wrench::S4U_Mailbox::igetMessage(this->mailbox);
            std::vector<wrench::S4U_PendingCommunication *> pending_comms = {pending.get()};
            auto index = wrench::S4U_PendingCommunication::waitForSomethingToHappen(pending_comms, 10);
            if (index != ULONG_MAX) {
                throw std::runtime_error("Should have gotten ULONG_MAX");
            }
        }

        return 0;
    }
};

TEST_F(S4U_MailboxTest, NetworkTimeout) {
    DO_TEST_WITH_FORK(do_NetworkTimeout_test);
}

void S4U_MailboxTest::do_NetworkTimeout_test() {


    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();

    int argc = 1;
    auto argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    //    argv[1] = strdup("--wrench-link-shutdown-simulation");
    //    argv[2] = strdup("--wrench-log-full");

    simulation->init(&argc, argv);

    // Setting up the platform
    simulation->instantiatePlatform(platform_file_path);

    // Create the WMSs
    auto workflow = wrench::Workflow::createWorkflow();
    this->wms1 = simulation->add(new NetworkTimeoutTestWMS(this, "Host1"));

    simulation->launch();

    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}


/**********************************************************************/
/**  NULL MAILBOX TEST                                               **/
/**********************************************************************/

class NullMailboxTestWMS : public wrench::ExecutionController {

public:
    NullMailboxTestWMS(S4U_MailboxTest *test,
                       const std::string &hostname) : wrench::ExecutionController(hostname, "test") {
        this->test = test;
    }


private:
    S4U_MailboxTest *test;
    std::string mode;

    int main() override {

        // Coverage
        wrench::S4U_Mailbox::putMessage(wrench::S4U_Mailbox::NULL_MAILBOX, nullptr);
        wrench::S4U_Mailbox::iputMessage(wrench::S4U_Mailbox::NULL_MAILBOX, nullptr);
        try {
            wrench::S4U_Mailbox::getMessage(wrench::S4U_Mailbox::NULL_MAILBOX);
            throw std::runtime_error("Shouldn't be able to get message from NULL_MAILBOX");
        } catch (std::invalid_argument &ignore) {}
        try {
            wrench::S4U_Mailbox::igetMessage(wrench::S4U_Mailbox::NULL_MAILBOX);
            throw std::runtime_error("Shouldn't be able to get message from NULL_MAILBOX");
        } catch (std::invalid_argument &ignore) {}

        return 0;
    }
};

TEST_F(S4U_MailboxTest, NullMailbox) {
    DO_TEST_WITH_FORK(do_NullMailbox_test);
}

void S4U_MailboxTest::do_NullMailbox_test() {


    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();

    int argc = 1;
    auto argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    //    argv[1] = strdup("--wrench-link-shutdown-simulation");
    //    argv[2] = strdup("--wrench-log-full");

    simulation->init(&argc, argv);

    // Setting up the platform
    simulation->instantiatePlatform(platform_file_path);

    // Create the WMSs
    this->wms1 = simulation->add(new NullMailboxTestWMS(this, "Host1"));

    simulation->launch();

    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}