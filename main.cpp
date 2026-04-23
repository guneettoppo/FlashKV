#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include<thread>
#include<vector>
#include <unordered_map>
#include <mutex>
#include <chrono>

using namespace std;

// Each key stores its value plus an optional expiry timestamp (in ms).
// expiry = -1 means the key does not expire.
struct Value{
    string data;
    long long expiry;

};


// Returns current monotonic time in milliseconds.
// steady_clock is used so TTL checks are not affected by system time changes.
long long now_ms() {
    return chrono::duration_cast<chrono::milliseconds>(
        chrono::steady_clock::now().time_since_epoch()
    ).count();
}

// Shared in-memory key/value store guarded by a mutex for thread safety.
unordered_map<string, Value> store;
mutex store_mutex;

class RespParser {
private:
    // Accumulates raw bytes from socket reads until full RESP frames are available.
    string buffer;

public:
    void append(const char* data, int len) {
        buffer.append(data, len);
    }

    
    bool parse(vector<string>& result) {
    result.clear();

    if (buffer.empty()) return false;

    // 🔥 HANDLE INLINE PROTOCOL FIRST (for redis-benchmark)
    if (buffer[0] != '*')
    {
        size_t pos = buffer.find("\r\n");
        if (pos == string::npos) return false;

        string line = buffer.substr(0, pos);
        buffer.erase(0, pos + 2);

        // split by space
        string word;
        for (char c : line)
        {
            if (c == ' ')
            {
                if (!word.empty()) result.push_back(word);
                word.clear();
            }
            else word += c;
        }
        if (!word.empty()) result.push_back(word);

        return true;
    }

        // Find end of first line (*<num>\r\n)
        size_t pos = buffer.find("\r\n");
        if (pos == string::npos) return false;

        int num_elements = stoi(buffer.substr(1, pos - 1));
        size_t idx = pos + 2;

        // Parse each bulk string argument from the array.
        for (int i = 0; i < num_elements; i++) {
            if (idx >= buffer.size()) return false;

            if (buffer[idx] != '$') return false;

            size_t len_end = buffer.find("\r\n", idx);
            if (len_end == string::npos) return false;

            int str_len = stoi(buffer.substr(idx + 1, len_end - idx - 1));
            idx = len_end + 2;

            // Wait for more data if current command frame is incomplete.
            if (idx + str_len + 2 > buffer.size()) return false;

            string arg = buffer.substr(idx, str_len);
            result.push_back(arg);

            idx += str_len + 2; // move past string + \r\n
        }

        // Remove parsed part from buffer
        buffer.erase(0, idx);

        return true;
    }
};    

void handle_client(int client_fd)
{
    // Per-client read buffer + parser state.
    char buf[1024];
    RespParser parser;

    while (true)
    {
        // Read bytes from this client socket.
        int bytes_received = recv(client_fd, buf, sizeof(buf), 0);
        if (bytes_received <= 0) break;

        parser.append(buf, bytes_received);

        vector<string> cmd;

        // Parse and execute every complete command currently available.
        // This supports pipelined requests in a single network read.
        while (parser.parse(cmd))
        {
            if (cmd.empty()) continue;

            string command = cmd[0];

            // Convert to uppercase (case-insensitive)
            for (auto &c : command) c = toupper((unsigned char)c);

            if (command == "PING")
            {
                string response = "+PONG\r\n";
                send(client_fd, response.c_str(), response.size(), 0);
            }
            else if (command == "ECHO")
            {
                if (cmd.size() < 2)
                {
                    string err = "-ERR wrong number of arguments\r\n";
                    send(client_fd, err.c_str(), err.size(), 0);
                    continue;
                }

                string msg = cmd[1];
                string response = "$" + to_string(msg.size()) + "\r\n" + msg + "\r\n";

                send(client_fd, response.c_str(), response.size(), 0);
            }
            else if (command == "SET")
{
    if (cmd.size() < 3)
    {
        string err = "-ERR wrong number of arguments\r\n";
        send(client_fd, err.c_str(), err.size(), 0);
        continue;
    }

    string key = cmd[1];
    string value = cmd[2];

    long long expiry = -1;

    if (cmd.size() >= 5)
    {
        string opt;
        for (char c : cmd[3]) opt += toupper((unsigned char)c);

        if (opt == "PX")
        {
            long long px = stoll(cmd[4]);
            expiry = now_ms() + px;
        }
    }

    {
        lock_guard<mutex> lock(store_mutex);
        store[key] = {value, expiry};
    }

    string response = "+OK\r\n";
    send(client_fd, response.c_str(), response.size(), 0);
}

else if (command == "CONFIG")
{
    // Simulate: CONFIG GET *
    string response =
        "*2\r\n"
        "$9\r\nmaxmemory\r\n"
        "$1\r\n0\r\n";

    send(client_fd, response.c_str(), response.size(), 0);
}


else if (command == "GET")
{
    if (cmd.size() < 2)
    {
        string err = "-ERR wrong number of arguments\r\n";
        send(client_fd, err.c_str(), err.size(), 0);
        continue;
    }

    string key = cmd[1];
    string value;
    bool found = false;

    {
        // Lock while reading/modifying shared store.
        lock_guard<mutex> lock(store_mutex);

        auto it = store.find(key);
        if (it != store.end())
        {
            // Lazy expiration: remove expired keys when accessed.
            if (it->second.expiry != -1 && now_ms() > it->second.expiry)
            {
                store.erase(it);
            }
            else
            {
                value = it->second.data;
                found = true;
            }
        }
    }

    if (found)
    {
        string response = "$" + to_string(value.size()) + "\r\n" + value + "\r\n";
        send(client_fd, response.c_str(), response.size(), 0);
    }
    else
    {
        string response = "$-1\r\n";
        send(client_fd, response.c_str(), response.size(), 0);
    }
}
else
{
    string err = "-ERR unknown command\r\n";
    send(client_fd, err.c_str(), err.size(), 0);
}
        }
    }

    // Client disconnected or socket read failed.
    close(client_fd);
}


int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  
    // Create a TCP socket for IPv4 connections.
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   std::cerr << "Failed to create server socket\n";
   return 1;
  }
  
  
  int reuse = 1;
    // Allows quick restart on the same port after process restarts.
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);
  
    // Bind server socket to 0.0.0.0:6379.
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  
    // Start listening for incoming client connections.
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }
  
  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  std::cout << "Waiting for a client to connect...\n";

  
  std::cout << "Logs from your program will appear here!\n";

   
    // Accept clients forever; each client is handled in a detached thread.
  while (true){
  int client_fd=accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t * )& client_addr_len);
  std:: cout<< "client connected\n";
  std:: thread(handle_client, client_fd).detach();
  }
  


 
  close(server_fd);

  return 0;
}
