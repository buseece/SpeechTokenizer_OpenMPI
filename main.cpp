#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <iterator>

using namespace std;

//Struct WordCount that stores a word and number of occurrences
struct WordCount
{
    int count; //initially 0
    char word[100]; //word is stored as char array
};


// qsort struct comparison function stated as its tutorial
int struct_cmp_by_word(const void *a, const void *b) {
    struct WordCount *ia = (struct WordCount *)a; //reference & dereference
    struct WordCount *ib = (struct WordCount *)b;
    return strcmp(ia->word, ib->word);//compares two char arrays
} 

int main(int argc, char *argv[])
{
    int size,rank; //number of processors, overall rank of a processor
    int inputLength; //length of tokenized input file

    int *counts=NULL; //stores number of words to-be-sent to a processor (Scatterv)
    int *displays=NULL; //stores starting index of words to-be-sent to a processor (Scatterv)

    int elements_per_proc; // input length / number of processors
    int max_elements_per_proc; // (input length / number of processors) +1 if remainder>0
    int rem; //input length % number of processors

    int sum=0; //not used
    
    //WordCount receiveBuffer[1000]; //processors' local receive buffer
    
    //MPI Initialization
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size); //initialize size
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); //initialize rank

    vector<string> inputs; //to get unknown number of input from the file
    
    //Master process gets input from the file
    if (rank == 0) {
        ifstream inFile("./speech_tokenized.txt"); //assumed the file name
        //string inputFile = argv[1];
        //ifstream inFile(inputFile.c_str());
        string input="";
        if(inFile.is_open()){ //if file is open
            while(inFile >> input){ //get new token
                inputs.push_back(input); //put it in inputs vector
            }
        }
        inputLength = inputs.size(); //input length is number of words
        if(inputLength == 0){ //if zero, exit
            cout<<"empty file "<<endl;
            return 1;
        }
        
    }

    //Declare struct to store words and their counts
    struct WordCount globalWords[inputLength];
    MPI_Status status;
    MPI_Datatype Particletype; //its type
    MPI_Datatype type[2] = { MPI_INT, MPI_CHAR }; //int and char[] thus MPI_INT, MPI_CHAR
    int blocklen[2] = { 1, 100 }; // will store 1 int and 100 chars
    MPI_Aint intex;
    MPI_Type_extent(MPI_INT, &intex); //size of int
    MPI_Aint disp[2]; //starting indexes
    disp[0] = (MPI_Aint) 0; //starts from 0
    disp[1] = intex; // starts after first field

    MPI_Type_create_struct(2, blocklen, disp, type, &Particletype); //create the struct
    MPI_Type_commit(&Particletype); //and commit to see

    counts = new int[size]; //number of words
    displays = new int[size]; //where it starts in the globalWords array

    if (rank == 0) {
        string try0="";
        int try1=0;
        //initialize array of struct wordCount
        for(int i=0; i<inputLength; i++){
            globalWords[i].count=0; //at the beginning their count is unknown
            strcpy(globalWords[i].word, inputs.at(i).c_str()); //take the char array
        }
    }
    
    //Broadcast the inputLength, # of elements per proc, max # of elements per proc to slaves
    MPI_Bcast(&inputLength, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);
    //Broadcast counts array to slaves
    //cout<<inputLength<<endl;
    elements_per_proc = inputLength / (size-1); // divide words to procs without processor 0
    rem = inputLength % (size-1); //remainder that will be distributed to words

    //calculate how many words that each process will take
    counts[0] =0; //0 processor will not calculate
    displays[0]=0; //0 processor will not calculate
    max_elements_per_proc = elements_per_proc;
    for(int i=1; i< size; i++){ // for each slave processor
        counts[i] = elements_per_proc; //if there was no remainder
        if(rem > 0){ //if there is a remainder give 1 to this processor
            counts[i]++;
            max_elements_per_proc = counts[i]; //this will be maximum number of words by any processor
            rem--;
        }
        displays[i]= sum; // stores index of start for this processor
        sum += counts[i];
    }
    
    WordCount receiveBuffer[max_elements_per_proc]; //processors' local receive buffer

    //if there will be idle processes exits
    for(int i=1;i<size; i++){
        if(counts[i]==0){ //that means processor with no input
            cout<<"more processes than tasks give less processes "<<endl;
            return 1; //return back
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

    //Split input data to slaves with respect to counts and displays
    MPI_Scatterv(&globalWords, counts, displays, Particletype, &receiveBuffer, counts[rank], Particletype, 0, MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);

    for(int i=0;i<counts[rank]; i++){
        receiveBuffer[i].count = 1; //change word counts to 1
    }

    //Gather all inputs from slaves
    MPI_Gatherv(receiveBuffer, counts[rank],  Particletype, globalWords, counts, displays, Particletype,0, MPI_COMM_WORLD);

    MPI_Barrier(MPI_COMM_WORLD);

    //Scatter again the array, with word counts 1
    MPI_Scatterv(globalWords, counts, displays, Particletype, receiveBuffer, counts[rank], Particletype, 0, MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);
    //For slaves
    if(rank!=0){
        int numberOfWords = counts[rank]; //number of words it received
        qsort( receiveBuffer, numberOfWords, sizeof *receiveBuffer, struct_cmp_by_word ); //sort the structs in alphabetical word order
    }
    
    MPI_Barrier(MPI_COMM_WORLD); //Waiting for each processor to finish before gathering
    
    //Gather all inputs from slaves, from localWords to globalWords
    MPI_Gatherv(receiveBuffer, counts[rank],  Particletype, globalWords, counts, displays, Particletype,0, MPI_COMM_WORLD);
    
    if(rank==0){
        //Sort gathered inputs at the master
        qsort( globalWords, inputLength, sizeof *globalWords, struct_cmp_by_word );
        //Reduce gathered and sorted input at master
        map<string,int> globalMap; //Create a map
        string _word="";
        int _count=0;
        for(int i=0; i<inputLength; i++){ //For each word
            string str(globalWords[i].word);
            _word = str; // take word
            _count = globalWords[i].count; //take its count
            int oldNum, newNum=0;
            if(globalMap.find(_word)!= globalMap.end()){ //if word is already added to map, update its number of occurrence
                oldNum= globalMap.find(_word)->second; //get old count
                newNum = oldNum + _count;
                globalMap.find(_word)->second = newNum; //update the count
            }else{ // word is not found in the map, add it
                globalMap.insert(make_pair(_word, _count));
            }
        }
        std::map<std::string, int>::iterator it = globalMap.begin(); //create iterator to move along the map
        while(it != globalMap.end()){ //while its not the end of the map
            std::cout<<it->first<<" :: "<<it->second<<std::endl; //write key :: value
            it++;
        }
    }
   
    MPI_Barrier(MPI_COMM_WORLD);
 
    MPI_Finalize();
    return 0;
}
