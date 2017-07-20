//
//  hash.c
//  The multilinear family hash function
//  hash max 32-length string
//

#include "hash.h"

int64_t* memory = NULL;


uint32_t initialize(uint32_t H){
    
    memory = malloc(sizeof(int64_t) * (32 + 1) * H);
    
    srand(373587883);
    int i;
    for(i = 0; i < 32 * H; i ++){
        uint64_t r1 = (uint64_t) rand();
        uint64_t r2 = (uint64_t) rand();
        memory[i] = r1 + (r2 << 32);
    }
    
    return 0;
}


uint32_t hash(char * str, uint32_t len, uint32_t h){
    int64_t* m = memory + h * (32 + 1);
    uint64_t sum = *(m++);

    int i;
    for(i = 0; i < len; ++m, ++str, ++i)
        sum+= *m * *str;
    
    return sum >> 32;
}


