#include <bits/fs_fwd.h>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <unordered_map>
#include <vector>
#include <unordered_set>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <stdio.h>
#include <cstdlib>
#include <unistd.h>
#include <algorithm>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"
#include "sns.pb.h"
#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"


namespace fs = std::filesystem;

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ClientContext;
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
using csce438::SynchronizerListReply;
using csce438::AllUsers;
// tl = timeline, fl = follow list
using csce438::TLFL;

int synchID = 1;
int clusterID = 1;
bool isMaster = false;
std::string coordAddr;
std::vector<std::string> otherHosts;
std::unordered_map<std::string, int> timelineLengths;

std::vector<std::string> get_lines_from_file(std::string);
void run_synchronizer(std::string,std::string,std::string,int);
std::vector<std::string> get_all_users_func(int);
std::vector<std::string> get_tl_or_fl(int, int, bool);
std::vector<std::string> getFollowersOfUser(int clientID);
bool file_contains_user(std::string filename, std::string user);
void updateTimelines(int id); 

void Heartbeat(std::string coordinatorIp, std::string coordinatorPort, ServerInfo serverInfo, int syncID);

std::unique_ptr<csce438::CoordService::Stub> coordinator_stub_;

class SynchServiceImpl final : public SynchService::Service {
    Status GetAllUsers(ServerContext* context, const Confirmation* confirmation, AllUsers* allusers) override{
        //std::cout<<"Got GetAllUsers"<<std::endl;
        std::vector<std::string> list = get_all_users_func(synchID);
        //package list
        for(auto s:list){
            allusers->add_users(s);
        }

        //return list
        return Status::OK;
    }

    Status GetFollowersOfClient(ServerContext*, const ID* id, AllUsers* allUsers) override {
        std::vector<std::string> followers = getFollowersOfUser(id->id());

        for (auto& follower : followers) {
            allUsers->add_users(follower);
        }

        return Status::OK;
    }

    Status GetTLFL(ServerContext* context, const ID* id, TLFL* tlfl){
        //std::cout<<"Got GetTLFL"<<std::endl;
        int clientID = id->id();

        std::vector<std::string> tl = get_tl_or_fl(synchID, clientID, true);
        std::vector<std::string> fl = get_tl_or_fl(synchID, clientID, false);

        //now populate TLFL tl and fl for return
        for(auto s:tl){
            tlfl->add_tl(s);
        }
        for(auto s:fl){
            tlfl->add_fl(s);
        }
        tlfl->set_status(true); 

        return Status::OK;
    }

    Status ResynchServer(ServerContext* context, const ServerInfo* serverinfo, Confirmation* c){
        std::cout<<serverinfo->type()<<"("<<serverinfo->serverid()<<") just restarted and needs to be resynched with counterpart"<<std::endl;
        std::string backupServerType;

        // YOUR CODE HERE


        return Status::OK;
    }

    Status SynchronizerList(ServerContext* context, const ID* id, SynchronizerListReply* lists) {
        std::cout << "Received SynchronizerList call from server" << std::endl;
        // Update user list with client IDs this synchronizer is responsible for
        std::vector<std::string> list = get_all_users_func(synchID);
        for(auto s:list){
            lists->add_all_users(s);
        }

        // Contact other synchronizers to get their client lists
        std::unique_ptr<CoordService::Stub> coord_stub_;
        coord_stub_ = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(coordAddr, grpc::InsecureChannelCredentials())));

        ID sID;
        sID.set_id(synchID);
        ServerList followerServers;
        ClientContext synchContext;
        coord_stub_->GetAllFollowerServers(&synchContext, sID, &followerServers);

        std::vector<std::string> hosts, ports;
        for (std::string host : followerServers.hostname()) {
            //std::cout << host << std::endl;
            hosts.push_back(host);
        }
        for (std::string port : followerServers.port()) {
            //std::cout << port << std::endl;
            ports.push_back(port);
        }
        if (hosts.size() != ports.size()) { // sizes should be the same -> we need hostname + port to contact the other follower synchronizers
            return Status::OK;
        }
        
        std::string targetHost;
        for (int i = 0; i < hosts.size(); i++) {
            targetHost = hosts.at(i) + ":" + ports.at(i);
            std::unique_ptr<SynchService::Stub> synch_stub_;
            synch_stub_ = std::unique_ptr<SynchService::Stub>(SynchService::NewStub(grpc::CreateChannel(targetHost, grpc::InsecureChannelCredentials())));

            ClientContext clientContext;
            AllUsers allUsers;
            Confirmation conf;
            //std::cout << "calling GetAllUsers to " << targetHost << std::endl;
            synch_stub_->GetAllUsers(&clientContext, conf, &allUsers);
            for (std::string user : allUsers.users()) {
                //std::cout << "User " << user << std::endl;
            }

            AllUsers allFollowers;
            ClientContext followerRequestContext;
            synch_stub_->GetFollowersOfClient(&followerRequestContext, *id, &allFollowers);
            for (auto follower : allFollowers.users()) {
                lists->add_followers(follower);
            }
        }

        return Status::OK;

    }
};

void RunServer(std::string coordIP, std::string coordPort, std::string port_no, int synchID){
  //localhost = 127.0.0.1
  std::string server_address("127.0.0.1:"+port_no);
  SynchServiceImpl service;
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

  std::thread t1(run_synchronizer,coordIP, coordPort, port_no, synchID);
  /*
  TODO List:
    -Implement service calls
    -Set up initial single heartbeat to coordinator
    -Set up thread to run synchronizer algorithm
  */

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}



int main(int argc, char** argv) {

    int opt = 0;
    std::string coordIP;
    std::string coordPort;
    std::string port = "3029";

    while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1){
        switch(opt) {
            case 'h':
                coordIP = optarg;
                break;
            case 'k':
                coordPort = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            case 'i':
                synchID = std::stoi(optarg);
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }

    coordAddr = coordIP + ":" + coordPort;
    clusterID = ((synchID-1) % 3) + 1;
    //std::cout << "cluster id " << std::to_string(((synchID-1) % 3) + 1) << std::endl;
    ServerInfo serverInfo;
    serverInfo.set_hostname("localhost");
    serverInfo.set_port(port);
    serverInfo.set_type("synchronizer");
    serverInfo.set_serverid(synchID);
    serverInfo.set_clusterid(clusterID);
    //std::thread sendHeartbeat(Heartbeat, coordIP, coordPort, serverInfo, synchID);
    Heartbeat(coordIP, coordPort, serverInfo, synchID);

    RunServer(coordIP, coordPort, port, synchID);
    return 0;
}

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID){
    //setup coordinator stub
    //std::cout<<"synchronizer stub"<<std::endl;
    std::string target_str = coordIP + ":" + coordPort;
    std::unique_ptr<CoordService::Stub> coord_stub_;
    coord_stub_ = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));
    //std::cout<<"MADE STUB"<<std::endl;

    ServerInfo msg;
    Confirmation c;

    msg.set_serverid(synchID);
    msg.set_hostname("127.0.0.1");
    msg.set_port(port);
    msg.set_type("follower");

    //send init heartbeat

    std::unique_ptr<SynchService::Stub> synch_stub_;
    //TODO: begin synchronization process
    while(true){
        //change this to 30 eventually
        sleep(5);

        grpc::ClientContext context;
        ServerList followerServers;
        ID id;
        id.set_id(synchID);

        coord_stub_->GetAllFollowerServers(&context, id, &followerServers);

        std::vector<std::string> hosts, ports;
        for (std::string host : followerServers.hostname()) {
            //std::cout << host << std::endl;
            hosts.push_back(host);
        }
        for (std::string port : followerServers.port()) {
            //std::cout << port << std::endl;
            ports.push_back(port);
        }
        if (hosts.size() != ports.size()) { // sizes should be the same -> we need hostname + port to contact the other follower synchronizers
            continue;
        }
        
        std::string targetHost;
        for (int i = 0; i < hosts.size(); i++) {
            targetHost = hosts.at(i) + ":" + ports.at(i);
            synch_stub_ = std::unique_ptr<SynchService::Stub>(SynchService::NewStub(grpc::CreateChannel(targetHost, grpc::InsecureChannelCredentials())));
            ClientContext clientContext;

            // Get all the users on other clusters
            AllUsers allUsers;
            Confirmation conf;
            //std::cout << "calling GetAllUsers to " << targetHost << std::endl;
            synch_stub_->GetAllUsers(&clientContext, conf, &allUsers);
            for (std::string user : allUsers.users()) {
                //std::cout << "User " << user << std::endl;
                std::string usersFile = "./cluster_" + std::to_string(clusterID) + "/1/all_users.txt";
                std::ofstream userStream(usersFile,std::ios::app|std::ios::out|std::ios::in);
                if (!file_contains_user(usersFile, user)) {
                    userStream << user << std::endl;
                }
            }

            // For each user in this cluster, find out which users on other clusters are following them
            for (auto client : get_all_users_func(synchID)) {
                ClientContext getFollowersContext;
                AllUsers allFollowers;
                ID id;
                id.set_id(atoi(client.c_str()));
                synch_stub_->GetFollowersOfClient(&getFollowersContext, id, &allFollowers);
                std::string followerFile = "./cluster_" + std::to_string(clusterID) + "/1/" + client + "_followers.txt";
                std::ofstream followerStream(followerFile,std::ios::app|std::ios::out|std::ios::in);
                for (auto follower : allFollowers.users()) {
                    if (!file_contains_user(followerFile, follower)) {
                        followerStream << follower << std::endl;
                    }
                }
                
                updateTimelines(atoi(client.c_str()));
            }

        }
        //synch all users file 
            //get list of all followers

            // YOUR CODE HERE
            //set up stub
            //send each a GetAllUsers request
            //aggregate users into a list
            //sort list and remove duplicates

            // YOUR CODE HERE

            //for all the found users
            //if user not managed by current synch
            // ...
 
            // YOUR CODE HERE

	    //force update managed users from newly synced users
            //for all users
//            for(auto i : aggregated_users){
                //get currently managed users
                //if user IS managed by current synch
                    //read their follower lists
                    //for followed users that are not managed on cluster
                    //read followed users cached timeline
                    //check if posts are in the managed tl
                    //add post to tl of managed user    
            
                     // YOUR CODE HERE
                    //}
                //}
//            }
    }
    return;
}

std::vector<std::string> get_lines_from_file(std::string filename){
  std::vector<std::string> users;
  std::string user;
  std::ifstream file; 
  file.open(filename);
  if(file.peek() == std::ifstream::traits_type::eof()){
    //return empty vector if empty file
    //std::cout<<"returned empty vector bc empty file"<<std::endl;
    file.close();
    return users;
  }
  while(file){
    getline(file,user);

    if(!user.empty())
      users.push_back(user);
  } 

  file.close();

  //std::cout<<"File: "<<filename<<" has users:"<<std::endl;
  /*for(int i = 0; i<users.size(); i++){
    std::cout<<users[i]<<std::endl;
  }*/ 

  return users;
}

void Heartbeat(std::string coordinatorIp, std::string coordinatorPort, ServerInfo serverInfo, int syncID) {
  std::string coordinatorInfo = coordinatorIp + ":" + coordinatorPort; 
  std::unique_ptr<CoordService::Stub> stub = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(coordinatorInfo, grpc::InsecureChannelCredentials())));

  ClientContext clientContext;
  csce438::Confirmation confirmation;
  stub->Heartbeat(&clientContext, serverInfo, &confirmation);
  if (!confirmation.status()) {
    //log(ERROR, "Failed to send heartbeat to coordinator.");
    //std::cout << "I am paired with the slave server" << std::endl;
    isMaster = false;
  } else {
    //std::cout << "I am paired with the master server" << std::endl;
    isMaster = true;
  }
  // Call Heartbeat RPC every five seconds
  /*while (true) {
    ClientContext newContext;
    ServerList serverList;
    stub->GetAllFollowerServers(&newContext, id, &serverList);
    for (auto& name : serverList.serverid()) {
        std::cout << std::to_string(name) << std::endl;         
    }
    sleep(5);
  }*/
}

bool file_contains_user(std::string filename, std::string user){
    std::vector<std::string> users;
    //check username is valid
    users = get_lines_from_file(filename);
    for(int i = 0; i<users.size(); i++){
      //std::cout<<"Checking if "<<user<<" = "<<users[i]<<std::endl;
      if(user == users[i]){
        //std::cout<<"found"<<std::endl;
        return true;
      }
    }
    //std::cout<<"not found"<<std::endl;
    return false;
}

std::vector<std::string> get_all_users_func(int synchID){
    //read all_users file master and client for correct serverID
    //std::string master_users_file = "./master"+std::to_string(synchID)+"/all_users";
    //std::string slave_users_file = "./slave"+std::to_string(synchID)+"/all_users";
    std::string clusterID = std::to_string(((synchID-1) % 3) + 1);
    std::string master_users_file = "./cluster_" + clusterID + "/1/all_users.txt";
    std::string slave_users_file = "./cluster_" + clusterID + "/2/all_users.txt";
    //take longest list and package into AllUsers message
    std::vector<std::string> master_user_list = get_lines_from_file(master_users_file);
    std::vector<std::string> slave_user_list = get_lines_from_file(slave_users_file);

    if(master_user_list.size() >= slave_user_list.size())
        return master_user_list;
    else
        return slave_user_list;
}

std::vector<std::string> get_tl_or_fl(int synchID, int clientID, bool tl){
    //std::string master_fn = "./master"+std::to_string(synchID)+"/"+std::to_string(clientID);
    //std::string slave_fn = "./slave"+std::to_string(synchID)+"/" + std::to_string(clientID);
    std::string master_fn = "cluster_"+std::to_string(clusterID)+"/1/"+std::to_string(clientID);
    std::string slave_fn = "cluster_"+std::to_string(clusterID)+"/2/"+std::to_string(clientID);
    if(tl){
        master_fn.append("_timeline.txt");
        slave_fn.append("_timeline.txt");
    }else{
        master_fn.append("_followers.txt");
        slave_fn.append("_followers.txt");
    }

    std::vector<std::string> m = get_lines_from_file(master_fn);
    std::vector<std::string> s = get_lines_from_file(slave_fn);

    if(m.size()>=s.size()){
        return m;
    }else{
        return s;
    }

}

void updateTimelines(int id) { // For client with id ID, update feeds of users on this cluster following given client(Only if given client is not in this cluster)
    int clientCluster = ((id-1) % 3) + 1;
    if (clientCluster == clusterID) { // Return, as it is not necessary to synchronize this timeline
        return;
    }

    ServerInfo server;
    ID clientID;
    clientID.set_id(id);
    ClientContext context;

    std::unique_ptr<CoordService::Stub> coord_stub_;
    coord_stub_ = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(coordAddr, grpc::InsecureChannelCredentials())));

    coord_stub_->GetFollowerServer(&context, clientID, &server);

    std::string targetSynchronizer = server.hostname() + ":" + server.port();
    std::unique_ptr<SynchService::Stub> synch_stub_;
    synch_stub_ = std::unique_ptr<SynchService::Stub>(SynchService::NewStub(grpc::CreateChannel(targetSynchronizer, grpc::InsecureChannelCredentials())));

    ClientContext timelineContext;
    TLFL tlfl;
    synch_stub_->GetTLFL(&timelineContext, clientID, &tlfl);
    std::vector<std::string> timeline;
    for (auto post : tlfl.tl()) {
        timeline.push_back(post);
    }
    // Now we will only append NEW posts to user feeds, that is, posts that have been made
    // since the previous timeline update. We do this by checking the previous timeline's size
    int previousTimelineSize;
    if (timelineLengths.find(std::to_string(id)) == timelineLengths.end()) {
        timelineLengths.insert({std::to_string(id), 0});
        previousTimelineSize = 0;
    } else {
        previousTimelineSize = timelineLengths.at(std::to_string(id));
    }
    for (auto user : get_all_users_func(synchID)) {
        std::string followingFile = "cluster_" + std::to_string(clusterID) + "/1/" + user + "_following.txt";
        std::string followListFile = "cluster_" + std::to_string(clusterID) + "/1/" + user + "_follow_list.txt";
        if (file_contains_user(followListFile, std::to_string(id))) {
            std::ofstream feed(followingFile,std::ios::app|std::ios::out|std::ios::in);
            for (int i = previousTimelineSize; i < timeline.size(); i++) {
                feed << timeline.at(i) << std::endl;
            }
        }
    }
    
    timelineLengths.at(std::to_string(id)) = timeline.size();
}

std::vector<std::string> getFollowersOfUser(int ID) {
    std::vector<std::string> followers;
    std::string clientID = std::to_string(ID);
    std::vector<std::string> usersInCluster = get_all_users_func(synchID);

    for (auto userID : usersInCluster) { // Examine each user's following file
        std::string file = "cluster_" + std::to_string(clusterID) + "/1/" + userID + "_follow_list.txt";
        //std::cout << "Reading file " << file << std::endl;
        if (file_contains_user(file, clientID)) {
            followers.push_back(userID);
        }
    }

    return followers;
}

