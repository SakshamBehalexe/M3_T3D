#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <pthread.h> 

#include <mpi.h>
#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <iterator>
#include <time.h>
#include <stdlib.h>


void InitializeMPI() {
    // Initialize the MPI environment
    MPI_Init(NULL, NULL); 
}

// Get the number of processes and rank
int GetProcessesCount() {
    int process_num;
    MPI_Comm_size(MPI_COMM_WORLD, &process_num); 
    return process_num;
}

int GetProcessId() {    
    int process_Id;
    MPI_Comm_rank(MPI_COMM_WORLD, &process_Id); // Get the rank of the process
    return process_Id;
}

std::string GetProcessName() {
    char processorName[MPI_MAX_PROCESSOR_NAME];
    int nameLen;
    MPI_Get_processor_name(processorName, &nameLen); // Get the name of the processor
    return std::string(processorName);
}

void MPIBroadcast(void *input, int inputSize, MPI_Datatype type, int root_process_id, MPI_Comm comm){
    MPI_Bcast(input, inputSize, type, root_process_id, comm); //Broadcast Matrix B
}

void MPISend(void *input, int inputSize, MPI_Datatype type, int dest_processId, MPI_Comm comm){
    MPI_Send(input, inputSize, type, dest_processId, 0, comm); //Send message to particular process
}

void MPIRecieve(void *buffer, int incomingDataCount, MPI_Datatype type, int sender_process_id, MPI_Comm comm){
    MPI_Recv(buffer, incomingDataCount, type, sender_process_id, 0, comm, MPI_STATUS_IGNORE); //Recieve message from particular process
}

void FinalizeMPI(){
    MPI_Finalize(); // Finalize the MPI environment.
}

using namespace std;

int PROD_TH;
int CONS_TH;
int QUEUE_SIZE;
 
struct TrafficSignal {
    int trafficId;
    int carsPassed;
    int timestamp;
};

struct ThreadArgs {
    int threadId;
    int lineId;
};

//Variables
vector<TrafficSignal> signals;
vector<TrafficSignal> signalQueue;
vector<TrafficSignal> results;
int CURRENT_ROW = 0;

//Thread Variables
vector<pthread_t> producerThreads;
vector<pthread_t> consumerThreads;
pthread_mutex_t accessLock;
MPI_Datatype MPI_DATASIGNAL;

// This function takes a string and a delimiter as input parameters
// and returns a vector of strings after splitting the input string
// into substrings based on the delimiter
vector<string> Split_string(string prompt, string delimiter){
    // Create a vector of strings
    vector<string> list;
    // Create a string object to store the input string
    string in_string = string(prompt);
    // Create a variable to store the position of the delimiter
    size_t pos = 0;
    // Create a variable to store the extracted substring
    string ex_string;
    // Loop until the delimiter is found or end of string is reached
    while ((pos = in_string.find(delimiter)) != string::npos) {
        // Extract the substring from start of string to delimiter
        ex_string = in_string.substr(0, pos);
        // Add the extracted substring to the vector
        list.push_back(ex_string);
        // Remove the extracted substring and delimiter from the original string
        in_string.erase(0, pos + delimiter.length());
    }
    // Add the last substring to the vector
    list.push_back(in_string);
    // Return the vector of substrings
    return list;
}

void generate_traffic() {
    //Seed random egenrator
    srand(time(NULL));

    ofstream myfile;
    myfile.open("traffic_data_mpi.txt");

    //Set starting timestamp and max number of traffic signals
    long currentTimestamp = time(NULL);
    const int MAX_ID = 12;

    for (int i = 0; i < 6000; i++) {
        currentTimestamp += 5;

        int id = rand() % MAX_ID;
        string signal = to_string(id) + "," + to_string(rand() % 5) + "," + to_string(currentTimestamp);

        myfile << signal + "\n";
    }

    myfile.close();
}

void LoadData() {
    ifstream myfile;
    myfile.open("traffic_data_mpi.txt");

    string line;

    while (getline(myfile, line)) {
        //Spilt line from TRAFFIC ID,CARS PASSED,TIMESTAMP
        vector<string> spilts = Split_string(line, ",");
            
        TrafficSignal sig = TrafficSignal();
        sig.trafficId = atoi(spilts[0].c_str());
        sig.carsPassed = atoi(spilts[1].c_str());
        sig.timestamp = atoi(spilts[2].c_str());

        signals.push_back(sig);
    }

    //Reverse array 
    reverse(signals.begin(), signals.end());

    CURRENT_ROW = results.size() - 1;

    cout<<"[MAIN] "<<to_string(signals.size()) << " signals has been loaded."<<endl;
}

bool compareSignals(TrafficSignal s1, TrafficSignal s2) { 
    return (s1.carsPassed > s2.carsPassed); 
}

void* Producer(void *input) {
    //Cast void pointer to correct data type
    ThreadArgs* args = (struct ThreadArgs*) input;

    while (true) {
        //Exit thread if no more signals to process
        if (signals.size() == 0) {
            pthread_exit(NULL);
        }

        //Lock all producer threads
        pthread_mutex_lock(&accessLock); 

        //Validation for recursive purposes
        if (!signals.empty() && signalQueue.size() < QUEUE_SIZE) {
            TrafficSignal &sig = signals[signals.size() - 1];
            signals.pop_back();

            //Add traffic signal to the queue
            signalQueue.push_back(sig);
            args->lineId = CURRENT_ROW++;
        }

        //Unlock producer threads
        pthread_mutex_unlock(&accessLock);
    }
}

void* Consumer(void *input) {
    //Cast void pointer to correct data type
    ThreadArgs* args = (struct ThreadArgs*) input;

    while (true) {
        //Exit thread if zero signals and queue is empty
        if (signals.size() == 0 && signalQueue.size() == 0){
            pthread_exit(NULL);
        }

        //Lock the consumer threads
        pthread_mutex_lock(&accessLock);

        if (signalQueue.size() > 0){
            TrafficSignal sig = signalQueue[0];
            signalQueue.erase(signalQueue.begin());

            bool exists = false;

            //Find the corrosponding traffic id from array and update it
            for (int i = 0; i < results.size(); i++) {
                if (sig.trafficId == results[i].trafficId){
                    results[i].carsPassed += sig.carsPassed;
                    exists = true;
                }
            }
            
            //Add traffic signal to array if it does not exist
            if (exists == false) {
                results.push_back(sig);
            }

            //Sort array
            sort(results.begin(), results.end(), compareSignals);
        }

        //Unlock the consumer threads
        pthread_mutex_unlock(&accessLock);
    }
}

void Create_producer() {
    //Initlaise thread array
    for (int i = 0; i < PROD_TH; i++) {
        pthread_t t;
        producerThreads.push_back(t);
    }

    //Starting threads
    for (int i = 0; i < PROD_TH; i++) {
        ThreadArgs *args = (struct ThreadArgs *) malloc(sizeof(struct ThreadArgs));
        args->threadId = i;
        args->lineId = CURRENT_ROW++;

        pthread_create(&producerThreads[i], NULL, Producer, (void *) args); 
    }
}

void Create_consumer() {

    for (int i = 0; i < CONS_TH; i++) {
        pthread_t t;
        consumerThreads.push_back(t);
    }


    for (int i = 0; i < CONS_TH; i++) {
        ThreadArgs *args = (struct ThreadArgs *) malloc(sizeof(struct ThreadArgs));
        args->threadId = i;
        args->lineId = 0;

        pthread_create(&consumerThreads[i], NULL, Consumer, (void *) args); 
    }
}

// Function to solve the problem by coordinating producer and consumer threads
void resolve() {

    // Initialize the access lock mutex
    if (pthread_mutex_init(&accessLock, NULL) != 0) { 
        printf("\n[MAIN]: Access Mutex lock init has failed.\n"); 
        return; 
    } 

    // Create consumer threads
    Create_consumer();
    // Create producer threads
    Create_producer();
    
    // Wait for producer threads to finish
    for (int i = 0; i < PROD_TH; i++){
        pthread_join(producerThreads[i], NULL);
    }

    // Wait for consumer threads to finish
    for (int i = 0; i < CONS_TH; i++){
        pthread_join(consumerThreads[i], NULL);
    }

    // Destroy the access lock mutex
    pthread_mutex_destroy(&accessLock);
}

// Function to calculate the chunk size based on the number of hours
// Parameters:
// - hours: The number of hours for which the chunk size is calculated
// Returns:
// - The chunk size, which is the total number of chunks for the given number of hours
int GetChunkSize(int hours) {
    return (12 * hours);
}

// Function to display a table row
void printTableRow(const std::string& col1, const std::string& col2) {
    const int colWidth = 18;
    std::cout << "| " << std::left << std::setw(colWidth) << col1;
    std::cout << "| " << std::left << std::setw(colWidth) << col2;
    std::cout << "|" << std::endl;
}

void Head() {
generate_traffic(); // Generate traffic data

LoadData(); // Load file into memory

int process_num = GetProcessesCount(); // Get the number of processes
const int chunkSize = GetChunkSize(24); // Calculate the chunk size based on the total number of signals and the number of processes
const int chunksCount = signals.size() / chunkSize; // Calculate the total number of chunks based on the signal size and chunk size
int chunkId = 0; // Initialize the chunk ID
int complete = 0; // Initialize the completion status

// Send Chunks to other processes
while (chunkId < chunksCount) {
    for (int pid = 0; pid < process_num; pid++) {
        if (pid == 0) continue; // Skip the first process (0)

        int chunkStart = (chunkId * chunkSize); // Calculate the start index of the current chunk
        int chunkEnd = chunkStart + chunkSize > signals.size() ? signals.size() : chunkStart + chunkSize; // Calculate the end index of the current chunk

        MPIBroadcast(&complete, 1, MPI_INT, 0, MPI_COMM_WORLD); // Send the current completion status to all processes

        vector<TrafficSignal> signalsChunk(signals.begin() + chunkStart, signals.begin() + chunkEnd); // Create a chunk of signals from the main signal vector

        int size = signalsChunk.size();
        MPISend(&size, 1, MPI_INT, pid, MPI_COMM_WORLD); // Send the size of the chunk of signals to the target process

        MPISend(&chunkId, 1, MPI_INT, pid, MPI_COMM_WORLD); // Send the ID of the current chunk to the target process

        MPISend(&signalsChunk[0], size, MPI_DATASIGNAL, pid, MPI_COMM_WORLD); // Send the signal data to the target process

        chunkId += 1; // Increment the chunk ID for the next iteration
    }
}

    // complete = 1;
    // MPIBroadcast(&complete, 1, MPI_INT, 0, MPI_COMM_WORLD);

    //Start listener thread
int numCongestedSignals;
cout << "Enter the number of most congested signals to display: ";
cin >> numCongestedSignals;

chunkId = 0;
while (true) {
    for (int pid = 0; pid < process_num; pid++) {
        if (pid == 0) continue; 

        int size;
        MPIRecieve(&size, 1, MPI_INT, pid, MPI_COMM_WORLD); //Send size of chunk of signals

        int subChunkId;
        MPIRecieve(&subChunkId, 1, MPI_INT, pid, MPI_COMM_WORLD); //Send id of current chunk

        results.clear();
        results.resize(size);
        MPIRecieve(&results[0], size, MPI_DATASIGNAL, pid, MPI_COMM_WORLD); //Send signal data
        chunkId += 1;

// Print top congested lights
            std::cout << "////////////////// DAY " << chunkId << "\\\\\\\\\\\\\\\\" << std::endl;

            // Table headers
            std::cout << "+-----------------+-----------------+" << std::endl;
            printTableRow("Traffic ID", "Number of Cars");
            std::cout << "+-----------------+-----------------+" << std::endl;

            // Table rows for congested signals
            for (int i = 0; i < numCongestedSignals && i < results.size(); i++) {
                std::string trafficId = "ID: " + std::to_string(results[i].trafficId);
                std::string carsPassed = std::to_string(results[i].carsPassed);
                printTableRow(trafficId, carsPassed);
            }

            std::cout << "+-----------------+-----------------+" << std::endl;
            std::cout << std::endl;
        }
        if (chunkId >= chunksCount) break;
    }

}

void Slave() {
    int process_Id = GetProcessId();
    int complete;
    int globalChunkId = 0;

    while (true) {
        MPIBroadcast(&complete, 1, MPI_INT, 0, MPI_COMM_WORLD);
        //printf("Process (%d) Completion status is: %d\n", process_Id, complete);

        if (complete == 1) {
            printf("Process (%d) is done...", process_Id);
            break;
        }

        //Recieve size of signals
        int size;
        MPIRecieve(&size, 1, MPI_INT, 0, MPI_COMM_WORLD);
        //printf("Process (%d) has incoming data of size %d\n", process_Id, size);

        int chunkId;
        MPIRecieve(&chunkId, 1, MPI_INT, 0, MPI_COMM_WORLD);

        //Buffer signals into array
        signals.clear();
        signals.resize(size);
        results.clear();
        results.resize(0);

        MPIRecieve(&signals[0], size, MPI_DATASIGNAL, 0, MPI_COMM_WORLD);

        resolve();

        sort(results.begin(), results.end(), compareSignals);

        //Send size of results array
        size = results.size();
        MPISend(&size, 1, MPI_INT, 0, MPI_COMM_WORLD);

        //Send chunk id
        MPISend(&chunkId, 1, MPI_INT, 0, MPI_COMM_WORLD);

        //Send results array
        MPISend(&results[0], size, MPI_DATASIGNAL, 0, MPI_COMM_WORLD);
    }
}
// Function to print a decorative line
void printDecorativeLine(int length, char symbol) {
    std::cout << std::string(length, symbol) << std::endl;
}

int main() {

    
    //Initlaise simulation paramaters
    srand(time(NULL));
    PROD_TH = (rand() % 8) + 2;
    CONS_TH = (rand() % 8) + 2;
    QUEUE_SIZE = (rand() % 20) + 10;

    //Initlaise MPI
    InitializeMPI();

    int process_Id = GetProcessId();

    //Create custom mpi data type
    const int nitems = 3;
    int blocklengths[nitems] = {1, 1, 1};
    MPI_Datatype types[nitems] = {MPI_INT, MPI_INT, MPI_INT};
    MPI_Aint items[nitems];

    items[0] = offsetof(TrafficSignal, trafficId);
    items[1] = offsetof(TrafficSignal, carsPassed);
    items[2] = offsetof(TrafficSignal, timestamp);

    MPI_Type_create_struct(nitems, blocklengths, items, types, &MPI_DATASIGNAL);
    MPI_Type_commit(&MPI_DATASIGNAL);

    if (process_Id == 0) {
        Head();
    }
    else {
        Slave();
    }

    if (process_Id == 0) {
        //Print simulation parameters
   // Print simulator parameters
    std::cout << std::endl;
    printDecorativeLine(54, '-');
    std::cout << "|         Simulator Parameters (Parallel)         |" << std::endl;
    printDecorativeLine(54, '-');
    std::cout << "| Producer Thread Count: " << std::setw(5) << PROD_TH << "              |" << std::endl;
    std::cout << "| Consumer Thread Count: " << std::setw(5) << CONS_TH << "              |" << std::endl;
    std::cout << "| Queue Size Count    : " << std::setw(5) << QUEUE_SIZE << "               |" << std::endl;
    printDecorativeLine(54, '-');
    std::cout << std::endl;
    }

    MPI_Type_free(&MPI_DATASIGNAL);
    FinalizeMPI();

    return 1;
}