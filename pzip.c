#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include<string.h>
#include <sys/mman.h> 
#include <pthread.h>
#include <sys/stat.h> 
#include <sys/sysinfo.h>
#include <unistd.h>

#define queueCapacity 10 //circular queue current size
int totalThreads=0; 
int pageSize = 4096; //Page size is 4096 Bytes
int numFiles=0; 
int isComplete=0; //Flag for sleeping threads 
int totalPages=0; //total pages required for the compressed output
int queueHead=0; 
int queueTail=0; 
int queueSize=0; //Circular queue capacity.
int* filePages;
int position=0;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER, filelock=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t empty = PTHREAD_COND_INITIALIZER, fill = PTHREAD_COND_INITIALIZER;

//Contains page specific data 
struct buffer {
    char* address; //address of  fileNumber file + pageNumber page
    int fileNumber; 
    int pageNumber; 
    int lastPageSize; 
}buf[queueCapacity];

//Contains file data for munmap
struct fd{
	char* addr;
	int size;
}*files;

struct output {
	char* data;
	int* count;
	int size;
}*out;

//Add index of the circular queue. 
void put(struct buffer b){
  	buf[queueHead] = b; //Enqueue the buffer
  	queueHead = (queueHead + 1) % queueCapacity;
  	queueSize++;
}

//Remove from the circular queue.
struct buffer get(){
  	struct buffer b = buf[queueTail]; //Dequeue the buffer.
	queueTail = (queueTail + 1) % queueCapacity;
  	queueSize--;
  	return b;
}

//Producer function for memory mapping
void* producer(void *arg){
	char** filenames = (char **)arg;
	struct stat sb;
	char* map; //mmap address
	int file;
	
	//Open the file
	for(int i=0;i<numFiles;i++){
		file = open(filenames[i], O_RDONLY);
		int pagesInFile=0; // # of pages = Size of file / Page size.
		int lastPageSize=0; 
		
		if(file == -1){ 
			printf("Error: File not found\n");
			exit(1);
		}

		//Get file info.
		if(fstat(file,&sb) == -1){ 
			close(file);
			printf("Error: Couldn't retrieve file stats");
			exit(1);
		}
		//Empty file
        	if(sb.st_size==0){
               		continue;
        	}
		//Calculate the number of pages and last page size.
		pagesInFile=(sb.st_size/pageSize);
		//Assign an extra page if the file is not aligned 
		if(((double)sb.st_size/pageSize)>pagesInFile){ 
			pagesInFile+=1;
			lastPageSize=sb.st_size%pageSize;
		}
		else{ 
			lastPageSize=pageSize;
		}
		totalPages+=pagesInFile;
		filePages[i]=pagesInFile;
		
		//Map the file.
		map = mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, file, 0); 															  
		if (map == MAP_FAILED) { 
			close(file);
			printf("Operation mmap failed\n");
			exit(1);
    	}	
    	
    	//Create buffer for file pages
		for(int j=0;j<pagesInFile;j++){
			pthread_mutex_lock(&lock);
			while(queueSize==queueCapacity){
			    pthread_cond_broadcast(&fill); //Wake up sleeping consumer threads.
				pthread_cond_wait(&empty,&lock); //Call consumer 
			}
			pthread_mutex_unlock(&lock);
			struct buffer temp;
			if(j==pagesInFile-1){ //Last page, might not be page-alligned
				temp.lastPageSize=lastPageSize;
			}
			else{
				temp.lastPageSize=pageSize;
			}
			temp.address=map;
			temp.pageNumber=j;
			temp.fileNumber=i;
			map+=pageSize; //Go to next page in the memory.
			pthread_mutex_lock(&lock);
			put(temp);
			pthread_mutex_unlock(&lock);
			pthread_cond_signal(&fill);
		}
		close(file);
	}
	isComplete=1; //producer is done mapping.
	pthread_cond_broadcast(&fill); //Wake up sleeping consumer threads.
	return 0;
}

//Compresses the buffer object.
struct output compression(struct buffer temp){
	struct output compressed;
	compressed.count=malloc(temp.lastPageSize*sizeof(int));
	char* tempString=malloc(temp.lastPageSize);
	int countIndex=0;
	for(int i=0;i<temp.lastPageSize;i++){
		tempString[countIndex]=temp.address[i];
		compressed.count[countIndex]=1;
		while(i+1<temp.lastPageSize && temp.address[i]==temp.address[i+1]){
			compressed.count[countIndex]++;
			i++;
		}
		countIndex++;
	}
	compressed.size=countIndex;
	compressed.data=realloc(tempString,countIndex);
	return compressed;
}

void *consumer(){
	do{
		pthread_mutex_lock(&lock);
		while(queueSize==0 && isComplete==0){
		    pthread_cond_signal(&empty);
			pthread_cond_wait(&fill,&lock); //call the producer to start filling the queue.
		}
		if(isComplete==1 && queueSize==0){ // producer is done mapping, nothing left in the queue.
			pthread_mutex_unlock(&lock);
			return NULL;
		}
		struct buffer temp=get();
		if(isComplete==0){
		    pthread_cond_signal(&empty);
		}	
		pthread_mutex_unlock(&lock);
		//Output position calculation
		
		for(int i=0;i<temp.fileNumber;i++){
			position+=filePages[i];
		}
		position+=temp.pageNumber;
	
		out[position]=compression(temp);
	}while(!(isComplete==1 && queueSize==0));
	return NULL;
}


int main(int argc, char* argv[]){
	//Check if less than two arguments
	if(argc<2){
		printf("pzip: file1 [file2 ...]\n");
		exit(1);
	}

	numFiles=argc-1; //Number of files, needed for producer.
	totalThreads=get_nprocs(); //Number of processes consumer threads 
	filePages=malloc(sizeof(int)*numFiles); //Pages per file.
	
    out=malloc(sizeof(struct output)* 512000*2); 
	//Create producer thread to map all the files.
	pthread_t pid,cid[totalThreads];
	pthread_create(&pid, NULL, producer, argv+1); 

	//Create consumer thread to compress all the pages per file.
	for (int i = 0; i < totalThreads; i++) {
        pthread_create(&cid[i], NULL, consumer, NULL);
    }

    //Wait for producer consumer to finish.
    for (int i = 0; i < totalThreads; i++) {
        pthread_join(cid[i], NULL);
    }
    pthread_join(pid,NULL);
    
    //print output 
	char* finalOutput=malloc(totalPages*pageSize*(sizeof(int)+sizeof(char)));
    char* init=finalOutput; 
	for(int i=0;i<totalPages;i++){
		if(i<totalPages-1){
			if(out[i].data[out[i].size-1]==out[i+1].data[0]){ 
				out[i+1].count[0]+=out[i].count[out[i].size-1];
				out[i].size--;
			}
		}
		
		for(int j=0;j<out[i].size;j++){
			int num=out[i].count[j];
			char character=out[i].data[j];
			*((int*)finalOutput)=num;
			finalOutput+=sizeof(int);
			*((char*)finalOutput)=character;
            finalOutput+=sizeof(char);
		}
	}
	fwrite(init,finalOutput-init,1,stdout);
	
	//free memory
	free(filePages);
	for(int i=0;i<totalPages;i++){
		free(out[i].data);
		free(out[i].count);
	}
	free(out);
	
	return 0;
}

