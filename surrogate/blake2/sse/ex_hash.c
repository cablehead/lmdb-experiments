#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "blake2.h"
#include "blake2-impl.h"
#include "blake2-config.h"

int main (int argc, char * argv[])
{
        int rc;
        char in_str[16], hash_str[16];
        printf("Enter in a string to be hashed: \n");
        scanf("%s", in_str);
        rc = blake2b ( hash_str, 16, in_str, strlen(in_str), NULL, 0);

        fprintf(stdout, "Hashed String: %s\n", hash_str);

        return 0;
}
                                                                                                                                                                                                           
