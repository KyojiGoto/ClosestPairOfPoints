#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "point.h"
#include "serial_closest.h"
#include "utilities_closest.h"


/*
 * Multi-process (parallel) implementation of the recursive divide-and-conquer
 * algorithm to find the minimal distance between any two pair of points in p[].
 * Assumes that the array p[] is sorted according to x coordinate.
 */
double closest_parallel(struct Point *p, int n, int pdmax, int *pcount) {
    // If there are 2 or 3 points, or if max number of worker processes is 0
    if (n <= 3 || pdmax == 0){
        return closest_serial(p, n);
    } else { //There are enough points to split into recursive processes
        int left_fd[2]; //left child pipe
        int right_fd[2]; //right child pipe
        int status; //status of child processes
        
        //Find the middle point
        int mid = n / 2;
        struct Point mid_point = p[mid];
        
        /* Create the left child pipe */
        if ((pipe(left_fd)) == -1) {
            perror("pipe");
            exit(1);
        }
        int l = fork();
        if(l < 0){
            //error
            perror("fork");
            exit(1);
        } else if(l == 0){
            //left child runs function
            double result = closest_parallel(p, mid, (pdmax - 1), pcount);
            //write answer to the pipe for parent to read
            write(left_fd[1], &result, sizeof(double));
            close(left_fd[1]);
            exit(*pcount);
        }
        
        /* Create the right pipe */
        if ((pipe(right_fd)) == -1) {
            perror("pipe");
            exit(1);
        }
        int r = fork();
        if(r < 0){
            //error
            perror("fork");
            exit(1);
        } else if(r == 0){
            //left child runs function
            double result = closest_parallel(p + mid, n - mid, (pdmax - 1), pcount);
            //write answer to the pipe for parent to read
            write(right_fd[1], &result, sizeof(double));
            close(right_fd[1]);
            exit(*pcount);
        }
        
        //parent runs
        //Results from running child process
        double dl;
        double dr;
        
        //pid of child
        int wpid;
        
        while((wpid = wait(&status)) > 0){
            if(wpid == l){ //if left child returns
                if (WIFEXITED(status)){
                    (*pcount) += WEXITSTATUS(status) + 1;
                    read(left_fd[0], &dl, sizeof(double));
                    close(left_fd[0]);
                }
            } else{ //right child returns
                if (WIFEXITED(status)){
                    (*pcount) += WEXITSTATUS(status) + 1;
                    read(right_fd[0], &dr, sizeof(double));
                    close(right_fd[0]);
                }
            }
        }
        
        // Find the smaller of two distances
        double d = min(dl, dr);
        
        
        // Build an array strip[] that contains points close (closer than d) to the line passing through the middle point.
        struct Point *strip = malloc(sizeof(struct Point) * n);
        if (strip == NULL) {
            perror("malloc");
            exit(1);
        }
    
        int j = 0;
        for (int i = 0; i < n; i++) {
            if (abs(p[i].x - mid_point.x) < d) {
                strip[j] = p[i], j++;
            }
        }
    
        // Find the closest points in strip.  Return the minimum of d and closest distance in strip[].
        double new_min = min(d, strip_closest(strip, j, d));
        free(strip);
        
        return new_min;
    }
    return 0.0;
}

