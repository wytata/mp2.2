/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <algorithm>
#include <ctime>

#include <exception>
#include <functional>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <filesystem>
#include <pthread.h>
#include <thread>
#include <semaphore.h>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.pb.h"
#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"


using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ClientContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::Message;
using csce438::ID;
using csce438::ListReply;
using csce438::SynchronizerListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;
using csce438::CoordService;
using csce438::ServerInfo;

struct Client {
  std::string username;
  bool connected = true;
  int following_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = 0;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

//Vector that stores every client that has been created
std::vector<Client*> client_db;
//Cluster ID
std::string clusterId, serverId;
bool isMaster = false;
std::unique_ptr<SNSService::Stub> slave_stub_ = nullptr; // slave stub used for replications of all master server interactions onto slave server

//Helper function used to find a Client object given its username
int find_user(std::string username){
  int index = 0;
  for(Client* c : client_db){
    if(c->username == username)
      return index;
    index++;
  }
  return -1;
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


  return users;
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

class SNSServiceImpl final : public SNSService::Service {
  
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
    log(INFO,"Serving List Request from: " + request->username()  + "\n");

    std::string allUsersFile = "all_users.txt";
    std::string user;
    std::ifstream file; 

    file.open(allUsersFile);
    if(file.peek() == std::ifstream::traits_type::eof()){
      file.close();
    }

    while(file){
      getline(file,user);
      if(!user.empty()) {
        list_reply->add_all_users(user);
      }
    } 
    file.close();
     
    std::string followersFile = request->username() + "_followers.txt";
    std::string follower;
    std::ifstream followerStream; 

    followerStream.open(followersFile);
    if(followerStream.peek() == std::ifstream::traits_type::eof()){
      followerStream.close();
    }

    while(followerStream){
      getline(followerStream,user);
      if(!user.empty()) {
        list_reply->add_followers(user);
      }
    } 
    followerStream.close();

    // Now incorporate the orginial logic to get clients from this cluster
    Client* client = client_db[find_user(request->username())];
 
    std::vector<Client*>::const_iterator it;
    for(it = client->client_followers.begin(); it!=client->client_followers.end(); it++){
      list_reply->add_followers((*it)->username);
    }

    return Status::OK;
    // Now we have all the users, followers from this cluster, but we need to talk to other clusters as well,
    // hence, we call SynchronizerList to our cluster's synchronizer to get the rest of the data for us 



    /*std::string clientID = request->username();
    std::string followersFile = clientID + "_followers.txt"; 
    std::vector<std::string> users;
    std::string userString;
    std::ifstream file; 
    file.open(filename);
    if(file.peek() == std::ifstream::traits_type::eof()){
      //return empty vector if empty file
      //std::cout<<"returned empty vector bc empty file"<<std::endl;
      file.close();
    } else {
      while(file){
        getline(file,userString);

        if(!user.empty())
          users.push_back(userString);
      } 
    }

    file.close();*/
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
    // First, replicate this RPC call onto the slave server, as we want it to maintain this following relationship
    ClientContext clientContext;
    if (slave_stub_ != NULL && isMaster) {
      slave_stub_->Follow(&clientContext, *request, reply);
    }

    std::string username1 = request->username();
    std::string username2 = request->arguments(0);
    log(INFO,"Serving Follow Request from: " + username1 + " for: " + username2 + "\n");

    if (username1 == username2) { // Don't allow this
      reply->set_msg("invalid username");
      return Status::OK;
    }

    int join_index = find_user(username2);
    if(join_index < 0) { // User on different cluster
      // First check all_users file for user

      if (!file_contains_user("all_users.txt", username2)) {
        reply->set_msg("invalid username");
        return Status::OK;
      }

      std::string filename = username1 + "_follow_list.txt";
      std::ofstream user_file(filename,std::ios::app|std::ios::out|std::ios::in);
      user_file << username2 << std::endl;
    } 

    else{
      Client *user1 = client_db[find_user(username1)];
      Client *user2 = client_db[join_index];      
      if(std::find(user1->client_following.begin(), user1->client_following.end(), user2) != user1->client_following.end()){
	      reply->set_msg("Join Failed -- Already Following User");
        return Status::OK;
      }
      user1->client_following.push_back(user2);
      user2->client_followers.push_back(user1);
      reply->set_msg("Follow Successful");

      /*std::string filename = username1 + "_following.txt";
      std::ofstream user_file(filename,std::ios::app|std::ios::out|std::ios::in);
      user_file << username2 << std::endl;*/
    }
    return Status::OK; 
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
    std::string username1 = request->username();
    std::string username2 = request->arguments(0);
    log(INFO,"Serving Unfollow Request from: " + username1 + " for: " + username2);
 
    int leave_index = find_user(username2);
    if(leave_index < 0 || username1 == username2) {
      reply->set_msg("Unknown follower");
    } else{
      Client *user1 = client_db[find_user(username1)];
      Client *user2 = client_db[leave_index];
      if(std::find(user1->client_following.begin(), user1->client_following.end(), user2) == user1->client_following.end()){
	reply->set_msg("You are not a follower");
        return Status::OK;
      }
      
      user1->client_following.erase(find(user1->client_following.begin(), user1->client_following.end(), user2)); 
      user2->client_followers.erase(find(user2->client_followers.begin(), user2->client_followers.end(), user1));
      reply->set_msg("UnFollow Successful");
    }
    return Status::OK;
  }

  // RPC Login
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {
    ClientContext clientContext;
    if (slave_stub_ != NULL && isMaster) {
      ClientContext duplicateContext;
      Request duplicateRequest;
      duplicateRequest.set_username(request->username());
      Reply duplicateReply;
      slave_stub_->Login(&duplicateContext, duplicateRequest, &duplicateReply);
    } else {
    }
    Client* c = new Client();
    std::string username = request->username();
    log(INFO, "Serving Login Request: " + username + "\n");
    
    int user_index = find_user(username);
    if(user_index < 0){
      c->username = username;
      client_db.push_back(c);
      reply->set_msg("Login Successful!"); 
      // Now update list of users in all_users.txt
      std::string filename = "all_users.txt";
      std::string semName = "/" + clusterId + "_" + serverId + "_all_users";
      sem_t* userSem = sem_open(semName.c_str(), O_CREAT);
      std::ofstream users_file(filename,std::ios::app|std::ios::out|std::ios::in);
      users_file << username << std::endl;
      sem_close(userSem);
    }
    else{
      Client *user = client_db[user_index];
      if(user->connected) {
	log(WARNING, "User already logged on");
        reply->set_msg("you have already joined");
      }
      else{
        std::string msg = "Welcome Back " + user->username;
	reply->set_msg(msg);
        user->connected = true;
      }
    }
    return Status::OK;
  }

  Status SlaveTimelineUpdate(ServerContext* context, const Message* message, Reply* reply) override {
    std::cout << "GOT SLAVE TIMELINE UPDATE" << std::endl;
    Client* c; 
    std::string username = message->username();
    c = client_db.at(find_user(username));

    if (strncmp("quit",message->msg().c_str(),4) == 0) {
      return Status::OK;
    }

    std::time_t timestamp_seconds = message->timestamp().seconds();
    std::tm* timestamp_tm = std::gmtime(&timestamp_seconds);
    char time_str[50]; // Make sure the buffer is large enough
    std::strftime(time_str, sizeof(time_str), "%a %b %d %T %Y", timestamp_tm);

    std::string ffo = username + '(' + time_str + ')' + " >> " + message->msg();

    // Append to user's timeline file
    std::ofstream userFile(username + "_timeline.txt", std::ios_base::app);
    if (userFile.is_open()) {
        userFile.seekp(0, std::ios_base::beg);
        userFile << ffo;
        userFile.close();
    }

    // Append to followers' following file 
    for (Client* follower : c->client_followers) {
        std::ofstream followerFile(follower->username + "_following.txt", std::ios_base::app);
        if (followerFile.is_open()) {
            followerFile.seekp(0, std::ios_base::beg);
            followerFile << ffo;
            followerFile.close();

        }
    }
    reply->set_msg("done");
    return Status::OK;
  }
    
  const int MAX_MESSAGES = 20;

  Status Timeline(ServerContext* context, 
		ServerReaderWriter<Message, Message>* stream) override { 

      // Initialize variables important for persisting timelines on the disk
      Message m;
      Client* c;
      std::string u;
      std::vector<std::string> latestMessages;
      std::vector<std::string> allMessages;
      bool firstTimelineStream = true;


      // multimap to fetch metadata from the servercontext which contains the username of the current client
      // this helps to Initialize the stream for this client as this is first contact
      const std::multimap<grpc::string_ref, grpc::string_ref>& metadata = context->client_metadata();

      auto it = metadata.find("username");
      if (it != metadata.end()) {
          std::string customValue(it->second.data(), it->second.length());

          // customValue is the username from the metadata received from the client
          u = customValue;
          //c = getClient(u);
          c = client_db.at(find_user(u));
          c->stream = stream; // set the client's stream to be the current stream
      }

      // if this is the first time the client is logging back 
      if (firstTimelineStream && c != nullptr) {
          // Read latest 20 messages from following file
          std::ifstream followingFile(u + "_following.txt");
          if (followingFile.is_open()) {
              std::string line;
              while (std::getline(followingFile, line)) {
                  allMessages.push_back(line);
              }

              // Determine the starting index for retrieving latest messages
              int startIndex = std::max(0, static_cast<int>(allMessages.size()) - MAX_MESSAGES);

              // Retrieve the latest messages
              for (int i = startIndex; i < allMessages.size(); ++i) {
                  latestMessages.push_back(allMessages[i]);
              }
              std::reverse(latestMessages.begin(), latestMessages.end()); // reversing the vector to match the assignment description
              followingFile.close();
          }

          // Send latest 20 messages to client via the grpc stream
          for (const std::string& msg : latestMessages) {
              Message latestMessage;
              latestMessage.set_msg(msg);
              stream->Write(latestMessage);
          }
          firstTimelineStream = false;
      }

      bool inTimeline = true;

      std::thread timelineThread([&]() {
        int previousLength; // Store previous length of user's timeline file. If it increased, then we know we have data to write to their stream
        std::string userFeed = u + "_following.txt";
        previousLength = get_lines_from_file(userFeed).size();
        while (inTimeline) {
          sleep(1);
          // Check timeline file for updates, write to stream accordingly
          std::vector<std::string> posts = get_lines_from_file(userFeed);
          if (posts.size() > previousLength) {
            for (int i = previousLength; i < posts.size(); i++) { // Write new posts to the stream
              Message post;
              post.set_msg(posts.at(i));
              stream->Write(post);
            }
            previousLength = posts.size();
          }
        }
      });

      while (stream->Read(&m)) { // while there are messages being sent by the client over the stream

          if (c != nullptr) {

              ClientContext slaveContext;
              Message slaveMessage;
              slaveMessage.set_msg(m.msg());
              slaveMessage.set_username(m.username());
              Reply slaveReply;
              if (slave_stub_ != NULL && isMaster) {
                std::cout << "calling SlaveTimelineUpdate" << std::endl;
                const Status s = slave_stub_->SlaveTimelineUpdate(&slaveContext, slaveMessage, &slaveReply);
                std::cout << slaveReply.msg() << std::endl;
                if (!s.ok()) {
                  std::cout << std::to_string(s.error_code()) << std::endl;
                  std::cout << "Couldn't execute slave update" << std::endl;
                }
              }
              // Convert timestamp to string
              std::time_t timestamp_seconds = m.timestamp().seconds();
              std::tm* timestamp_tm = std::gmtime(&timestamp_seconds);

              //std::cout << "SERVER GOT MESSAGE: " << m.msg() << std::endl;

              log(INFO, m.msg());
              if (strncmp("quit",m.msg().c_str(),4) == 0) {
                std::cout << "WARNING: CLIENT TERMINATED" << std::endl;
                inTimeline = false;
                break;
              }

              char time_str[50]; // Make sure the buffer is large enough
              std::strftime(time_str, sizeof(time_str), "%a %b %d %T %Y", timestamp_tm);

              std::string ffo = u + '(' + time_str + ')' + " >> " + m.msg();

              // Append to user's timeline file
              std::ofstream userFile(u + "_timeline.txt", std::ios_base::app);
              if (userFile.is_open()) {
                  userFile.seekp(0, std::ios_base::beg);
                  userFile << ffo;
                  userFile.close();
              }


              // Send the new message to all followers for their timeline
              for (Client* follower : c->client_followers) {
                  if (follower->stream != nullptr) {
                      Message followerMessage;
                      followerMessage.set_msg(ffo);

                      if (follower->stream != nullptr) {
                          //follower->stream->Write(followerMessage);
                      } 

                  } 
              }

              // Append to  all the followers' following file
              for (Client* follower : c->client_followers) {
                  std::ofstream followerFile(follower->username + "_following.txt", std::ios_base::app);
                  if (followerFile.is_open()) {
                      followerFile.seekp(0, std::ios_base::beg);
                      followerFile << ffo;
                      followerFile.close();
                  }
              }
          } 

      }

      timelineThread.join();
      return Status::OK;
  }
  /*
    log(INFO,"Serving Timeline Request");
    Message message;
    Client *c;
    while(stream->Read(&message)) {
      std::string username = message.username();
      int user_index = find_user(username);
      c = client_db[user_index];

      std::cout << "SERVER GOT MESSAGE " << message.msg() << std::endl;
 
      //Write the current message to "username.txt"
      std::string filename = "timeline_" + username +".txt";
      std::ofstream user_file(filename,std::ios::app|std::ios::out|std::ios::in);
      google::protobuf::Timestamp temptime = message.timestamp();
      std::string time = google::protobuf::util::TimeUtil::ToString(temptime);
      std::string fileinput = time+" :: "+message.username()+":"+message.msg()+"\n";
      //"Set Stream" is the default message from the client to initialize the stream
      if(message.msg() != "Set Stream")
        user_file << fileinput;
      //If message = "Set Stream", print the first 20 chats from the people you follow
      else{
        if(c->stream==0)
      	  c->stream = stream;
        std::string line;
        std::vector<std::string> newest_twenty;
        std::ifstream in(username+"following.txt");
        int count = 0;
        //Read the last up-to-20 lines (newest 20 messages) from userfollowing.txt
        while(getline(in, line)){
//          count++;
//          if(c->following_file_size > 20){
//	    if(count < c->following_file_size-20){
//	      continue;
//            }
//          }
          newest_twenty.push_back(line);
        }
        Message new_msg; 
 	//Send the newest messages to the client to be displayed
 	if(newest_twenty.size() >= 40){ 	
	    for(int i = newest_twenty.size()-40; i<newest_twenty.size(); i+=2){
	       new_msg.set_msg(newest_twenty[i]);
	       stream->Write(new_msg);
	    }
        }else{
	    for(int i = 0; i<newest_twenty.size(); i+=2){
	       new_msg.set_msg(newest_twenty[i]);
	       stream->Write(new_msg);
	    }
        }
        //std::cout << "newest_twenty.size() " << newest_twenty.size() << std::endl; 
        continue;
      }
      //Send the message to each follower's stream
      std::vector<Client*>::const_iterator it;
      for(it = c->client_followers.begin(); it!=c->client_followers.end(); it++){
        Client *temp_client = *it;
      	if(temp_client->stream!=0 && temp_client->connected)
        std::cout << "---\n\n\nWRITING MESSAGE TO CLIENT\n\n\n------";
        temp_client->stream->Write(message);
        //For each of the current user's followers, put the message in their following.txt file
        std::string temp_username = temp_client->username;
        std::string temp_file = temp_username + "following.txt";
        std::ofstream following_file(temp_file,std::ios::app|std::ios::out|std::ios::in);
        following_file << fileinput;
        temp_client->following_file_size++;
        std::ofstream user_file(temp_username + ".txt",std::ios::app|std::ios::out|std::ios::in);
        user_file << fileinput;
      }
    }
    //If the client disconnected from Chat Mode, set connected to false
    c->connected = false;
    return Status::OK;
  }*/

};

void RunServer(std::string port_no) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on "+server_address);

  server->Wait();
}

void Heartbeat(std::string coordinatorIp, std::string coordinatorPort, ServerInfo serverInfo) {
  std::string coordinatorInfo = coordinatorIp + ":" + coordinatorPort; 
  std::unique_ptr<CoordService::Stub> stub = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(coordinatorInfo, grpc::InsecureChannelCredentials())));

  // Call Heartbeat RPC every five seconds
  while (true) {
    sleep(5);
    ClientContext clientContext;
    csce438::Confirmation confirmation;
    stub->Heartbeat(&clientContext, serverInfo, &confirmation);
    isMaster = confirmation.status();
    if (isMaster) {
      // Need to get slave's info
      ClientContext getSlaveContext;
      ID id;
      ServerInfo slaveInfo;
      id.set_id(atoi(clusterId.c_str()));
      stub->GetSlave(&getSlaveContext, id, &slaveInfo);
      if (!slaveInfo.hostname().empty() && slave_stub_ == NULL) {
        std::cout << "GOT SLAVE AT " << slaveInfo.hostname() + ":" + slaveInfo.port() << std::endl;
        slave_stub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(grpc::CreateChannel(slaveInfo.hostname() + ":" + slaveInfo.port(), grpc::InsecureChannelCredentials())));

      }
    }
  }
}

int main(int argc, char** argv) {

  clusterId = "1";
  serverId = "1";
  std::string coordinatorIp = "localhost";
  std::string coordinatorPort = "9090";
  std::string port = "3010";
  
  int opt = 0;
  while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1){
    switch(opt) {
      case 'c':
        clusterId = optarg;break;
      case 's':
        serverId = optarg;break;
      case 'h':
        coordinatorIp = optarg;break;
      case 'k':
        coordinatorPort = optarg;break;
      case 'p':
          port = optarg;break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }
  
  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");

  ServerInfo serverInfo;
  serverInfo.set_hostname("localhost");
  serverInfo.set_port(port);
  serverInfo.set_type("server");
  serverInfo.set_serverid(atoi(serverId.c_str()));
  serverInfo.set_clusterid(atoi(clusterId.c_str()));
  std::thread sendHeartbeat(Heartbeat, coordinatorIp, coordinatorPort, serverInfo);

  // Create server's directory for timeline storage if it doesn't already exist
  std::string serverDirectory = "cluster_" + clusterId;
  struct stat statBuf;
  if (stat(serverDirectory.c_str(), &statBuf) != 0) {
    mkdir(serverDirectory.c_str(), 0755);
  }
  // Change directories so that timeline files will be placed in the server's directory
  chdir(serverDirectory.c_str());

  serverDirectory = serverId;
  if (stat(serverDirectory.c_str(), &statBuf) != 0) {
    mkdir(serverDirectory.c_str(), 0755);
  }

  chdir(serverDirectory.c_str());

  RunServer(port);
  sendHeartbeat.join();

  return 0;
}
//-- vim: ts=2 sts=2 sw=2 et
