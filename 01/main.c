#include <aio.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <ucontext.h>

#define stack_size 1024 * 1024
#define nbytes 1024

#define yield() ({\
    int last_ci = sheduler.curr_ci;\
    sheduler.curr_ci = (sheduler.curr_ci+1) % (sheduler.coros_n+1);\
    if (!sheduler.curr_ci) { sheduler.curr_ci++; }\
    sheduler.coros[last_ci].ttime +=\
    clock() - sheduler.coros[last_ci].work;\
    sheduler.coros[sheduler.curr_ci].work = clock();\
    swapcontext(\
        &sheduler.coros[last_ci].context,\
        &sheduler.coros[sheduler.curr_ci].context\
        );\
})

struct coroutine {
    ucontext_t context;
    char* stack;
    clock_t work, ttime;
};

struct sheduler {
    int curr_ci;
    int coros_n;
    int working_coros;
    struct coroutine *coros;

} sheduler;

struct array {
    int* array;
    int len;
};

struct data {
    int files_n;
    char** files;
    struct array* sorted;

} data;

char* async_read(char* filename) {
    yield();
    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        printf("Can't open file %s!\n", filename);
        exit(EXIT_FAILURE);
    }
    yield();
    struct aiocb cb;
    int bs = nbytes;
    yield();
    char* buffer = (char*)malloc(bs * sizeof(char));
    if (buffer == NULL) {
        printf("Malloc error!\n");
        exit(EXIT_FAILURE);
    }
    yield();
    memset(&cb, 0, sizeof(struct aiocb));
    cb.aio_fildes = fd;
    cb.aio_offset = 0;
    cb.aio_nbytes = nbytes;
    cb.aio_buf = buffer;
    yield();
    while (1) {
        yield();
        if (aio_read(&cb) == -1) {
            printf("Unable to create request!\n");
            free(buffer);
            close(fd);
            exit(EXIT_FAILURE);
        }
        yield();
        while (aio_error(&cb) == EINPROGRESS);
        yield();
        int nb = aio_return(&cb);
        if (nb == -1) {
            printf("Error happened while aio_return!\n");
            free(buffer);
            close(fd);
            exit(EXIT_FAILURE);
        }
        yield();
        cb.aio_offset += nb;
        yield();
        if (bs % cb.aio_offset == 0) {
            yield();
            bs = cb.aio_offset + nbytes;
            yield();
            buffer = (char*)realloc(buffer, bs);
            yield();
            if (buffer == NULL) {
                printf("Realloc error!\n");
                exit(EXIT_FAILURE);
            }
        } else {
            yield();
            bs = cb.aio_offset + 1;
            yield();
            buffer = (char*)realloc(buffer, bs);
            if (buffer == NULL) {
                printf("Realloc error!\n");
                exit(EXIT_FAILURE);
            }
            yield();
            buffer[bs-1] = '\0';
            yield();
            close(fd);
            yield();
            return buffer;
        }
        cb.aio_buf = buffer + cb.aio_offset;
        yield();
    }
}

void merge(int arr[], int lb, int md, int rb) {
    int s1 = md - lb + 1;
    int s2 = rb - md;
    int larr[s1], rarr[s2];

    for (int i = 0; i < s1; i++) {
        larr[i] = arr[lb+i];
    }

    for (int j = 0; j < s2; j++) {
        rarr[j] = arr[md+j+1];
    }

    int i = 0, j = 0, k = lb;
    while (i < s1 && j < s2) {
        if (larr[i] <= rarr[j]) {
            arr[k++] = larr[i++];
        } else {
            arr[k++] = rarr[j++];
        }
    }

    while (i < s1) {
        arr[k++] = larr[i++];
    }

    while (j < s2) {
        arr[k++] = rarr[j++];        
    }
    
    return;
}

void merge_sort(int arr[], int lb, int rb) {
    if (lb < rb) {
        yield();
        int md = lb + (rb-lb) / 2;
        yield();
        merge_sort(arr, lb, md);
        yield();
        merge_sort(arr, md+1, rb);
        yield();
        merge(arr, lb, md, rb);
        yield();
    }
    yield();
    return;
}

void print_array(int arr[], int len) {
    for (int i = 0; i < len; i++) {
        printf("%d ", arr[i]);
    }
    printf("\n");

    return;
}

int* convert(char* p, int* l) {
    int res_i = 1;
    yield();
    int* res = (int*)malloc(res_i * sizeof(int));
    yield();
    char *head = p;
    yield();
    char ch[5];
    yield();
    int ch_i = 0;
    yield();    
    while (*p != '\0') {
        yield();
        if (*p == ' ') {
            yield();
            p++;
            yield();
            res[res_i++-1] = atoi(ch);
            yield(); 
            res = (int*)realloc(res, res_i * sizeof(int));
            yield();
            memset(ch, '\0', 5);
            yield(); 
            ch_i = 0;
            yield();
        }
        yield();
        ch[ch_i++] = *p++;
        yield();
    }
    yield();
    res[res_i-1] = atoi(ch);
    yield();
    *l = res_i;
    yield();
    memset(ch, '\0', 5);
    yield();
    free(head);
    yield();
    return res;
}

void merge_two_arrays(struct array *a1, struct array *a2) {
    int *p1 = a1->array, *p2 = a2->array;
    int *p3 = (int*)malloc((a1->len+a2->len) * sizeof(int)), *head = p3;

    while (p1 < a1->array+a1->len && p2 < a2->array+a2->len) {
        if (*p1 <= *p2) { *p3++ = *p1++; }
        else { *p3++ = *p2++; }
    }
    
    while (p1 < a1->array + a1->len) { *p3++ = *p1++; }
    while (p2 < a2->array + a2->len) { *p3++ = *p2++; }
    free(a1->array);
    free(a2->array);
    a1->array = head;
    a1->len += a2->len;
}

void merge_arrays(struct array *arrays, size_t arrays_n) {
    if	(arrays_n < 2) return;
    merge_arrays(arrays, arrays_n / 2);
    merge_arrays(arrays + arrays_n/2, arrays_n - arrays_n/2);
    merge_two_arrays(&arrays[0], &arrays[arrays_n/2]);
}

void sort() {
    sheduler.working_coros++;
    yield();
    char *res = async_read(data.files[sheduler.curr_ci-1]);
    yield();
    struct array result;
    yield();
    result.len = 1;
    yield();
    result.array = convert(res, &result.len);
    yield();
    merge_sort(result.array, 0, result.len-1);
    yield();
    data.sorted[sheduler.curr_ci-1].array = result.array;
    yield();
    data.sorted[sheduler.curr_ci-1].len = result.len;
    yield();
    sheduler.working_coros--;
    while (sheduler.working_coros) { yield(); }
    return;
}

void* allocate_stack() {
    void* stack = (void*)malloc(stack_size);
    stack_t ss;
    ss.ss_sp = stack;
    ss.ss_size = stack_size;
    ss.ss_flags = 0;
    sigaltstack(&ss, NULL);
    return stack;
}

void init() {
    sheduler.coros = (struct coroutine*)malloc(
        (sheduler.coros_n+1) * sizeof(struct coroutine));
    for (int i = 1; i <= sheduler.coros_n; i++) {
        if (getcontext(&sheduler.coros[i].context) == -1) {
            printf("Can't get context");
            exit(EXIT_FAILURE);
        }
        sheduler.coros[i].ttime = 0;

        sheduler.coros[i].work = 0;
        sheduler.coros[i].stack = allocate_stack();
        sheduler.coros[i].context.uc_stack.ss_sp = sheduler.coros[i].stack;
        sheduler.coros[i].context.uc_stack.ss_size = stack_size;
        sheduler.coros[i].context.uc_link = &sheduler.coros[0].context;
        makecontext(&sheduler.coros[i].context, (void (*)(void))sort, 0);

    }

    sheduler.coros[1].work = clock();
    return;
}

void write_to() {
    FILE* fd = fopen("result", "w");
    for (int i = 0; i < data.sorted[0].len; i++) {
        fprintf(fd, "%d ", data.sorted[0].array[i]);
    }

    fclose(fd);
    return;
}

void duration() {
    for (int i = 1; i <= sheduler.coros_n; i++) {
        printf("Coro %d executed in %f seconds.\n", i, (double)sheduler.coros[i].ttime / CLOCKS_PER_SEC);
    }
    printf("\n");
}

void free_all() {
    for (int i = 1; i <= sheduler.coros_n; i++) {
        free(sheduler.coros[i].stack);
    }

    free(sheduler.coros);
    free(data.sorted[0].array);
    free(data.sorted);
    
}

int main(int argc, char** argv) {
    if (argc < 2) {
        printf("Invalid command line arguments.\n");
        exit(EXIT_FAILURE);
    }

    sheduler.curr_ci = 1;
    sheduler.coros_n = argc - 1;
    sheduler.working_coros = 0;

    data.files = &argv[1];
    data.files_n = argc - 1;
    data.sorted = (struct array*)malloc(data.files_n * sizeof(struct array));

    printf("Starting sorting files...\n");
    clock_t start = clock();

    init();
    swapcontext(&sheduler.coros[0].context, &sheduler.coros[1].context);
    merge_arrays(data.sorted, sheduler.coros_n);
    write_to();
    duration();
    free_all();

    clock_t end = clock();
    printf("Program executed in %f seconds\n",
            (double)(end-start) / CLOCKS_PER_SEC);

    return 0;
}