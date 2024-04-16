#include <algorithm>
#include <cstdio>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;
using csce438::ServerList;
using csce438::SynchService;

struct zNode{
    int serverID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive();

};

//potentially thread safe 
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;

// creating a vector of vectors containing znodes
std::vector<std::vector<zNode*>> clusters = {cluster1, cluster2, cluster3};

std::mutex s_mutex;
std::vector<zNode*> synchronizer1;
std::vector<zNode*> synchronizer2;
std::vector<zNode*> synchronizer3;

// creating a vector of vectors containing synchronizers
std::vector<std::vector<zNode*>> synchronizers = {synchronizer1, synchronizer2, synchronizer3};


//func declarations
int findServer(std::vector<zNode*> v, int id); 
std::time_t getTimeNow();
void checkHeartbeat();


bool zNode::isActive(){
    bool status = false;
    if(!missed_heartbeat){
        status = true;
    }else if(difftime(getTimeNow(),last_heartbeat) < 10){
        status = true;
    }
    return status;
}


class CoordServiceImpl final : public CoordService::Service {

    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
        int clusterID = serverinfo->clusterid();
        int serverID = serverinfo->serverid();
        std::string serverType = serverinfo->type();

        log(INFO, "Received heartbeat from server " + std::to_string(serverID) + " of type " + serverType + " in cluster " + std::to_string(clusterID) + ".");
        
        int serverIndex;
        if (serverType == "synchronizer") { // Synchronizers send a single heartbeat at startup, announcing their existence to the coordinator
          s_mutex.lock();
          serverIndex = findServer(synchronizers.at(clusterID-1), serverID);
          if (serverIndex == -1) { // synchronizer doesn't yet exist in coordinator database.
            std::cout << "adding new synchronizer server to database" << std::endl;
            zNode* toAdd = new zNode();
            toAdd->serverID = serverinfo->serverid();
            toAdd->port = serverinfo->port();
            toAdd->hostname = serverinfo->hostname();
            toAdd->type = serverinfo->type();
            synchronizers.at(clusterID-1).push_back(toAdd);
          }
          s_mutex.unlock();
          confirmation->set_status(true);
          return Status::OK;
        }

        v_mutex.lock();
        serverIndex = findServer(clusters.at(clusterID-1), serverID);
        
        if (serverIndex == -1) {
          // Server not found in cluster -> first heartbeat from server, so it must be initialized
          zNode* toAdd = new zNode();
          toAdd->serverID = serverinfo->serverid();
          toAdd->port = serverinfo->port();
          toAdd->hostname = serverinfo->hostname();
          toAdd->type = serverinfo->type();
          toAdd->last_heartbeat = getTimeNow();
          toAdd->missed_heartbeat = false;

          clusters.at(clusterID-1).push_back(toAdd);

        } else {
          zNode* targetServer = clusters.at(clusterID-1).at(serverIndex);
          targetServer->missed_heartbeat = false;
          targetServer->last_heartbeat = getTimeNow();
        }


        v_mutex.unlock();
        confirmation->set_status(true);

        return Status::OK;
    }

    //function returns the server information for requested client id
    //this function assumes there are always 3 clusters and has math
    //hardcoded to represent this.
    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        int clientId = id->id();
        int clusterId = ((clientId - 1) % 3) + 1;
        int serverId = 1;

        v_mutex.lock();
        auto cluster = clusters.at(clusterId-1);
        if (clusters.at(clusterId-1).size() == 0) {
          serverinfo->set_serverid(-1);
          log(INFO, "No server available for client in cluster " + std::to_string(clusterId));
        } else {
          zNode* targetServer = clusters.at(clusterId-1).at(serverId-1); 
          //zNode* targetServer = cluster.at(findServer(cluster, serverId)); 
          serverinfo->set_port(targetServer->port);
          serverinfo->set_type(targetServer->type);
          serverinfo->set_hostname(targetServer->hostname);
          serverinfo->set_serverid(targetServer->serverID);
          serverinfo->set_clusterid(clusterId);
          log(INFO, "Directed client " + std::to_string(clientId) + " to server " + std::to_string(targetServer->serverID) + " in cluster " + std::to_string(clusterId));

        }
        v_mutex.unlock();


        return Status::OK;
    }

    Status GetAllFollowerServers(ServerContext* context, const ID* id, ServerList* serverList) override {
      int clusterID = id->id();
      std::cout << "request from syncrhonizer " << std::to_string(clusterID) << std::endl;

      for (int i = 0; i < synchronizers.size(); i++) {
        std::cout << "got into loop\n";
        if (i != clusterID - 1) { // Synchronizer calling RPC does not need synchronizer info from its own cluster
          for (auto& synchronizer : synchronizers.at(i)) {
            std::cout << "got into inner loop\n";
            serverList->add_port(synchronizer->port);
            serverList->add_type(synchronizer->type);
            serverList->add_serverid(synchronizer->serverID);
            serverList->add_hostname(synchronizer->hostname);
          }
        }
      }
      return Status::OK;
    }
};

void RunServer(std::string port_no){
    //start thread to check heartbeats
    std::thread hb(checkHeartbeat);
    //localhost = 127.0.0.1
    std::string server_address("127.0.0.1:"+port_no);
    //std::string server_address("192.168.122.46:"+port_no);
    CoordServiceImpl service;
    //grpc::EnableDefaultHealthCheckService(true);
    //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {

    std::string port = "3010";
    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1){
        switch(opt) {
            case 'p':
                port = optarg;
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }
    std::string log_file_name = std::string("coordinator-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Server starting...");
    RunServer(port);
    return 0;
}

int findServer(std::vector<zNode*> v, int id) {
  int result = -1;
  for (int i = 0; i < v.size(); i++) {
    if (id == v.at(i)->serverID) {
      result = i;
    }
  }

  return result;
}

void checkHeartbeat(){
    while(true){
        //check servers for heartbeat > 10
        //if true turn missed heartbeat = true
        // Your code below

        v_mutex.lock();

        // iterating through the clusters vector of vectors of znodes
        for (auto& c : clusters){
            for(auto& s : c){
                if(difftime(getTimeNow(),s->last_heartbeat)>10){
                    std::cout << "missed heartbeat from server " << s->serverID << std::endl;
                    if(!s->missed_heartbeat){
                        s->missed_heartbeat = true;
                        s->last_heartbeat = getTimeNow();
                    }
                }
            }
        }

        v_mutex.unlock();

        sleep(3);
    }
}


std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}
//-- vim: ts=2 sts=2 sw=2 et
