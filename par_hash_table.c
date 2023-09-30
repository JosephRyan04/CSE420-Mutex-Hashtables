#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/stat.h>
#include <string.h>
#include <math.h>
/*
 * CSE420 Project 3 Spring 23
 * Joseph Ryan
 * 3/5/23
 */

//Nodes which go into hashtable
// key is the information being stored in the hashtable, value is the number of times a key has been inserted
typedef struct Node{
    struct Node *nextNode;
    int key,value;

}Node;


Node *create_node(){
    Node *node = calloc(1,sizeof(Node));
    node->key = -1;
    node->nextNode = NULL;
    return node;
}

//Used to initialize empty nodes in the hashtable
Node blank_node(){
    Node node;
    node.value = 0;
    node.key = -1;
    node.nextNode = NULL;
    return node;
}

//Global variables here
int N = 0;
int lock_size = 0;
Node *hashtable;
pthread_mutex_t *locks;


void insert_hash(int key){

    int i = key % N;
    int lock_id = floor((i/lock_size));

    //Lock mutex lock
    pthread_mutex_lock(&locks[lock_id]);

    //If the hashtable position is empty (denoted by a -1 key ,all positions are initialized as such), then
    //initialize the first node
    if(hashtable[i].key == -1){
        hashtable[i].key = key;
        hashtable[i].value++;
    }
    //Else, there is a collision at position
    else{
        //In which case, iterate through the linked list until you reach a node with the same key, or the list end
        Node *curnode = &hashtable[i];
        while(curnode->nextNode != NULL && curnode->key != key){
            curnode = curnode->nextNode;
        }
        //Same key in node, increment value to denote there are 'value' # of keys in the node so far
        if(curnode->key == key){
            curnode->value++;
        }
        //Otherwise no node found with same key, so create one
        else{
            curnode->nextNode = create_node();
            curnode->nextNode->value = 1;
            curnode->nextNode->key = key;
        }
    }


    //Unlock mutex
    pthread_mutex_unlock(&locks[lock_id]);

}


//Searches the directory located in dir_path for all files which are not folders or subdirectories
//The names of each file is stored in buffer, which can hold up to 512 file names (one per 'string pointer')
//Each file name is dynamically allocated
int filenames(char *dir_path, char *buffer[512]){
    DIR *directory;
    struct dirent *file;
    struct stat filetype = {};

    int nFiles =0;
    directory = opendir(dir_path);
    if(directory == NULL) printf("Failed open folder\nRemove line 188 - possible quirk of filesystem\n");
    while( (file = readdir(directory)) != NULL ) {
        stat(file->d_name,&filetype);

        //If file is not directory, alloc the space for the filename appended to the dir_path and add to buffer
        if( ! S_ISDIR(filetype.st_mode) ){
            buffer[nFiles] = calloc(strlen(file->d_name) + strlen(dir_path) + 2, sizeof(char));
            strcat(buffer[nFiles],dir_path);
            strcat(buffer[nFiles],file->d_name);
            nFiles++;
        }
   }
    free(directory);
    return nFiles;

}

//iterates through all nodes in the hashtable, if value matches the correct # of occurrences, append to file
void file_output(char *filename, int nEntries, int value){
    FILE *output = fopen(filename, "a+");
    for(int i = 0; i < nEntries; i++) {
        Node *curnode = &hashtable[i];

            if (curnode->value == value) {
                fprintf(output, "%d\n", curnode->key);
            }
        while (curnode->nextNode != NULL) {
            curnode = curnode->nextNode;
            if (curnode->value == value) {
                fprintf(output, "%d\n", curnode->key);
            }
        }
    }
    fclose(output);
}

//Function used by each thread
//Takes in a file name as argument passes each line to the insert_hash
void *file_to_hash(void *param){

    char *line = NULL;
    int key;
    size_t length = 0;


    FILE* file = fopen(param,"r");
    if(file == NULL){printf("fopen on %s failed!",(char *)param); exit(-1);}
    while(getline(&line,&length,file) != -1){
        key = atoi(line);
        insert_hash(key);
    }
    free(line);
    fclose(file);
    pthread_exit(0);

}

//For each file found, a thread is created and the filename is passed to file_to_hash
void create_threads(int num_threads, void * function, char *args[]){
    pthread_t tid[num_threads];
    for(int i = 0; i < num_threads; i++){
        pthread_create(&tid[i], NULL, function, args[i]);
    }
    for(int i = 0; i < num_threads; i++){
        pthread_join(tid[i], NULL);
    }
}

int main(int argc, char *argv[]) {

    int value;
    int nLocks;
    int nEntries;
    char *dir_path;
    char *buffer[512]; //Holds the space fo 512 string pointers, the space for the strings itself is dynamic
    char *output_path;

    //Handles input arguments
    if (argc != 6){
        printf("Invalid number of inputs!\n");
        exit(-1);
    }
    else {
        value = atoi(argv[5]);
        nLocks = atoi(argv[2]);
        nEntries = atoi(argv[1]);
        output_path = calloc(strlen(argv[4]) + 1, 1);
        strcat(output_path,argv[4]);
        dir_path = calloc(strlen(argv[3]) + 2, 1);
        strcat(dir_path,argv[3]);
        
        //Please comment out the strcat if path reading issues
        strcat(dir_path,"/");
    }

    //Initialize hashtable and locks dynamically based on input args
    //Escapes scope
    hashtable = calloc(nEntries, sizeof(Node));
    for(int i = 0; i < nEntries; i++){hashtable[i] = blank_node();}

    locks = calloc(nLocks, sizeof(pthread_mutex_t));
    for(int i =0; i < nLocks; i++){pthread_mutex_init(&locks[i],NULL);}

    N = nEntries;
    lock_size = ceil(N / nLocks);
    int num_threads = filenames(dir_path,buffer);

    //Thread creation function, args are the number of files/threads, hash file to hash parser function, and filenames
    create_threads(num_threads, file_to_hash, buffer);

    file_output(output_path,nEntries,value);


    //Memory management
    Node *lead,*trail;
    for (int i = 0; i < nEntries; ++i) {
        if(hashtable[i].nextNode != NULL) {
           trail = hashtable[i].nextNode;
           while(trail->nextNode != NULL){
               lead = trail->nextNode;
               free(trail);
               trail = lead;
           }
            free(trail);
        }
    }
    free(hashtable);

    for(int i = 0; i < nLocks; i ++){
        pthread_mutex_destroy(&locks[i]);
    }

    for (int i = 0; i < num_threads; ++i) {
        free(buffer[i]);
    }
    free(locks);
    free(output_path);
    free(dir_path);
    return 0;
}

