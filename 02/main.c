#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <setjmp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define MAX_COMMAND_LENGTH 1024
#define MAX_STR 1024
#define INVITE "$ "

char buffer[MAX_COMMAND_LENGTH]; // Command buffer
char token[MAX_COMMAND_LENGTH+2]; // Current token
int cursor = 0; // Current position in token string

jmp_buf point;

struct pid_bg { // Background process PID
    pid_t pid;
    struct pid_bg *next_pid;
};

struct pid_bg *bg_list = NULL; // Background processes list

struct cmd { // Command struct
    char **argv; // Command name and arguments

    char *input_file; // <
    char *output_file; // > and >>
    int append; // 1 if >>, 0 if >

    int background; // = 1, if command must be executed in background

    int and;  // = 1, if separator is &&
    int or; // = 1, if separator is ||

    struct cmd *subcmd; // (subcommand)
    struct cmd *next; // next command after ';'
    struct cmd *pipe; // next command after '|'
};

struct cmd *head; // head to struct command tree

char *ss;
int ns;

void free_memory(struct cmd *head); // free

int error(char *message, int fatal) { // error handler
    fprintf(stderr, "%s\n", message);
    if (fatal) {
        free_memory(head);
        longjmp(point, 1);
    }

    return 1;
}

void add_process(pid_t process) {
    struct pid_bg **p = &bg_list;
    while (*p) {
        p = &((*p)->next_pid);
    }

    *p = (struct pid_bg *)malloc(sizeof(struct pid_bg));
    (*p)->pid = process;
    (*p)->next_pid = NULL;

    return;
}

void delete_process(pid_t process) {
    struct pid_bg **p = &bg_list;
    struct pid_bg *target;
    while (*p) {
        if ((*p)->pid == process) {
            target = *p;
            *p = (*p)->next_pid;
            free(target);
            return;
        }
        p = &((*p)->next_pid);
    }

    error("Delete process error: not found process in list.", 1);
}

void print_list() {
    struct pid_bg **p = &bg_list;
    int status;
    while (*p) {
        waitpid((*p)->pid, &status, WNOHANG);
        if (WIFEXITED(status)) {
            // printf("Done: %d\n", (*p)->pid);
            delete_process((*p)->pid);
            continue;
        } else {
            // printf("%d\n", (*p)->pid);
        }
        p = &((*p)->next_pid);
    }

    return;
}

void print_cmd(struct cmd *this, char *which) {
    printf("\n--------------------");
    printf("\n%s\n\n", which);

    int i = 0;
    while (this->argv[i] != NULL) {
        printf("[%s] ", this->argv[i++]);
    }
    printf("\n");
    printf("Input file: %s\n", this->input_file);
    printf("Output file: %s\n", this->output_file);
    printf("Append mode: %d\n", this->append);
    printf("Background: %d\n", this->background);
    printf("And: %d\n", this->and);
    printf("Or: %d\n", this->or);
    if (this->subcmd)
        printf("Subcommand: %s\n", *this->subcmd->argv);
    if (this->pipe)
        printf("Pipe: %s\n", *this->pipe->argv);
    if (this->next)
        printf("Next: %s\n", *this->next->argv);
    printf("--------------------\n");

    return;
}

int check_sys_cmd(char **command) {
    if (!command)
        return 1;
    if (!strcmp(*command, "exit"))
        exit(0);
    if (!strcmp(*command, "cd")) {
        if (chdir(command[1]))
            return error("Cd error: arguments are not correct.", 0);
        else
            return 0;
    }

    return 1;
}

void slash(char *str) {
    char *p = str;
    for (int i = 0; i < MAX_STR; i++) {
        if (p[i] == '\\') {
            if (p[i+1] == ' ') {
                p[i+1] = '#';
            } else {
                if (p[i+1] == '#') {
                    p[i+1] = ' ';
                }
            }
        }
    }

    return;
}

void newl(char *str) {
    char *p = str;
    for (int i = 0; i < MAX_STR; i++) {
            if (p[i] == '$') {
                p[i] = '\n';
            }
    }
}

int skipto(char *str, char *sep) {
    for (int i = 0; i < MAX_STR; ++i) {
        if (str[i] == 0)
            return 0;
        for (int j = 0; j < strlen(sep); ++j) {
            if (str[i] == sep[j])
                return i;
        }
    }

    return error("Skip error: bad syntax in command.", 1);
}

int skipon(char *str,char *sep) {
    int k;
    for (int i = 0; i < MAX_STR; ++i) {
        k = 0;
        if (str[i] == 0)
            return 0;
        for (int j = 0; j < strlen(sep); ++j) {
            if (str[i] == sep[j])
                ++k;
        }

        if (k == 0)
            return i;
    }

    return error("Skip error: bad syntax in command.",1);
}

void get_token() {
    int tmp;

    // printf("\n---get_token()---\n");

    if ((buffer[cursor] == '&' && buffer[cursor+1] == '&') ||
        (buffer[cursor] == '|' && buffer[cursor+1] == '|')) {
        token[0] = buffer[cursor];
        token[1] = buffer[cursor+1];
        token[2] = '\n';
        token[3] = '\0';
        cursor += 2;
        // printf("1. new token: [%s]\n", token);
        return;
    }

    if (buffer[cursor] == ';' ||
        buffer[cursor] == '&' ||
        buffer[cursor] == '|' ||
        buffer[cursor] == '(' ||
        buffer[cursor] == ')' ||
        buffer[cursor] == '\n') {
        token[0] = buffer[cursor];
        token[1] = '\n';
        token[2] = '\0';
        if (buffer[cursor] != '\n')
            ++cursor;
        // printf("2. new token: [%s]\n", token);
        return;
    }

    tmp = skipto(buffer+cursor, ";&|()\n");
    if (tmp)
        strncpy(token, buffer+cursor, tmp);
    token[tmp] = '\n';
    token[tmp+1] = '\0';
    cursor += tmp;
    // printf("3. new token: [%s]\n", token);
    return;
}

char **get_name(int size, char *cmd) {
    char **tmp = (char **)malloc(2 * sizeof(char *));
    *tmp = (char *)malloc((size+1) * sizeof(char));
    strncpy(*tmp, cmd, size);
    (*tmp)[size] = '\0';
    *(tmp+1) = NULL;

    return tmp;
}

char **get_arg(char **head, int size, char *cmd) {
    int i = 0;
    char **tmp = head;
    while (*(tmp++))
        ++i;
    tmp = (char **)realloc(head, (i+2) * sizeof(char *));
    *(tmp+i) = (char *)malloc(sizeof(char) * (size+1));

    strncpy(*(tmp+i), cmd, size);
    (*(tmp+i))[size] = '\0';
    *(tmp+i+1) = NULL;

    return tmp;
}

struct cmd *get_command();

struct cmd *new_command() {
    struct cmd *tmp = (struct cmd *)malloc(sizeof(struct cmd));
    tmp->argv = 0;
    tmp->input_file = 0;
    tmp->output_file = 0;
    tmp->append = 0;
    tmp->background = 0;
    tmp->and = 0;
    tmp->or = 0;
    tmp->subcmd = 0;
    tmp-> pipe = 0;
    tmp->next = 0;

    return tmp;
}

struct cmd *get_simple_command() {
    struct cmd *this = new_command();
    int len;
    int end = 0;
    char in_or_out;
    char *command = token;


    // printf("\n---Start get_simple_command()---\n");

    this->input_file = 0;
    this->output_file = 0;
    this->append = 0;
    len = skipon(command, " ");
    end += len;
    len = skipto(command+end, " <>|\n");
    if (len == 0)
        error("Empty command.", 1);
    this->argv = get_name(len, command+end);
    // print_cmd(this, "1. get_simple_command::this");
    end += len;
    slash(command+end);
    ss = command+end;
    while ((len = skipon(command+end, " \n"))) {
        end += len;
        if ((command[end] == '<') || (command[end] == '>') || (command[end] == '|'))
            break;
        int flag = 0;
        if (command[end] == '"') {
            flag = 1;
            for (int i = 1; i < MAX_STR; i++) {
                if (command[end+i] != '\0')
                    // printf("[%c]\n", command[end+i]);
                if (command[end+i] == '"') {
                    len = i-1;
                    end += 1;
                    break;
                }
            }
        } else if (command[end] == '\'') {
            flag = 1;
            for (int i = 1; i < MAX_STR; i++) {
                if (command[end+i] == '\'') {
                    len = i-1;
                    end += 1;
                    break;
                }
            }
        } else
            len = skipto(command+end, " <>|\n");
        this->argv = get_arg(this->argv, len, command+end);
        // print_cmd(this, "2. get_simple_command::this");
        end += len;
        if (flag) {
            end++;
            len++;
            flag = 0;
        }
    }

    while (command[end] == '>' || command[end] == '<') {
        in_or_out = command[end];
        ++end;
        if (in_or_out == '>' && command[end] == '>') {
            ++end;
            this->append = 1;
        }
        len = skipon(command+end, " ");
        end += len;

        int flag = 0;
        if (command[end] == '"') {
            flag = 1;
            for (int i = 1; i < MAX_STR; i++) {
                if (command[end+i] == '"') {
                    len = i-1;
                    end += 1;
                    break;
                }
            }
        } else if (command[end] == '\'') {
            flag = 1;
            for (int i = 1; i < MAX_STR; i++) {
                if (command[end+i] == '\'') {
                    len = i-1;
                    end += 1;
                    break;
                }
            }
        } else
            len = skipto(command+end, " <>|\n");

        if (len == 0)
            error("No argument after '<' or '>' symbols.", 1);
        if (in_or_out == '>') {
            if (this->output_file)
                error("Too many output files.", 1);
            this->output_file = (char *)malloc((len+1) * sizeof(char));
            strncpy(this->output_file, command+end, len);
            (this->output_file)[len] = '\0';
        }
        if (in_or_out == '<') {
            if (this->input_file)
                error("Too many input files.", 1);
            this->input_file = (char *)malloc((len+1) * sizeof(char));
            strncpy(this->input_file, command+end, len);
            (this->input_file)[len] = '\0';
        }
        end += len;
        len = skipon(command+end, " ");
        end += len;
    }

    // printf("\n---End get_simple_command()---\n");
    return this;
}

struct cmd *get_conveyor() { // Выделяем конвейеры
    struct cmd *this;
    struct cmd *tmp;

    // printf("\n---Start get_conveyor()---\n");
    this = get_command();
    this->pipe = 0;
    tmp = this;
    while (!strcmp(token, "|\n")) {
        get_token();
        tmp->pipe = get_command();
        tmp = tmp->pipe;
        tmp->pipe = 0;
    }

    // printf("\n---End conveyor()---\n");
    return this;
}

struct cmd *get_list_of_commands() { // Выделяем список команд.
    struct cmd *this;
    struct cmd *tmp;

    // printf("\n---Start get_list_of_commands()---\n");

    this = get_conveyor();
    // print_cmd(this, "get_list_of_commands()::this");
    this->next = 0;
    tmp = this;
    while (!strcmp(token, ";\n") || !strcmp(token, "&\n") || !strcmp(token, "&&\n") || !strcmp(token, "||\n")) {
        if (!strcmp(token, "&\n"))
            tmp->background = 1;
        if (!strcmp(token, "&&\n"))
            tmp->and = 1;
        if (!strcmp(token, "||\n"))
            tmp->or = 1;

        get_token();
        
        if (token[0] == ')' || token[0] == '\n')
            return this;
        tmp->next = get_conveyor();
        tmp = tmp->next;
        tmp->next = 0;
    }
    // print_cmd(this, "get_list_of_commands()::this");

    // printf("\n---End get_list_of_commands()---\n");
    return this;
}

struct cmd *get_command() {
    struct cmd *this;

    // printf("\n---Start get_command()---\n");

    if (token[0] == '(') {
        get_token();
        this = new_command();
        this->subcmd = get_list_of_commands();
        if (token[0] != ')')
            error("Bracket imbalance.", 1);
    } else
        this = get_simple_command();
    get_token();

    // printf("\n---End get_command()---\n");
    return this;
}

struct cmd *parse() {
    // printf("\n---Start parse()---\n");

    struct cmd *parsed;
    parsed = get_list_of_commands();
    // print_cmd(parsed, "parse()::parsed");
    if (token[0] != '\n')
        error("Bad tail.",0);

    // printf("\n---End parse()---\n\n\n");
    return parsed;
}

int execute (struct cmd *cmdstruc, int ispipe) {
    int status;
    int fd[2];
    int in, out;
    pid_t curpid;

    if (cmdstruc) {
        if (cmdstruc->argv || cmdstruc->subcmd) {
            if (cmdstruc->pipe) {
                if(pipe(fd)) {
                    sleep(1);
                    error("Pipe error.", 1);
                }
            }
            if (check_sys_cmd(cmdstruc->argv)) {
                if (!(curpid = fork())) {
                    if (cmdstruc->pipe) {
                        if (cmdstruc->output_file) {
                            error("Output file error.", 0);
                            free(cmdstruc->output_file);
                            cmdstruc->output_file = 0; 
                        }
                        dup2(fd[1], 1);
                        close(fd[0]);
                        close(fd[1]);
                    }
                    if (ispipe) {
                        if (cmdstruc->input_file) {
                            error("Input file error.", 0);
                            free(cmdstruc->input_file);
                            cmdstruc->input_file = 0; 
                        }
                        dup2(ispipe, 0);
                        close(ispipe);
                    }
                    if (cmdstruc->input_file) {
                        if ((in = open(cmdstruc->input_file, O_RDONLY)) == -1) {
                            free(cmdstruc->input_file);
                            cmdstruc->input_file = 0;
                            error("Input file error.", 0);
                        } else {
                            dup2(in, 0);
                            close(in);
                        }
                    }
                    if (cmdstruc->output_file) {
                        if ((out = open(cmdstruc->output_file, O_WRONLY | O_CREAT | (cmdstruc->append ? O_APPEND : O_TRUNC), 0777)) == -1) {
                            free(cmdstruc->output_file);
                            cmdstruc->output_file = 0; 
                            error("Output file error.", 0);
                        } else {
                            dup2(out, 1);
                            close(out);
                        }
                    }
                    if (cmdstruc->argv) {
                        if (cmdstruc->argv[1] != NULL) {
                            slash(cmdstruc->argv[1]);
                            newl(cmdstruc->argv[1]);
                        }
                        if (execvp(cmdstruc->argv[0], cmdstruc->argv))
                            printf("%s - unknown command\n", cmdstruc->argv[0]);
                    } else
                        if (cmdstruc->subcmd)
                            execute(cmdstruc->subcmd, 0);
                    exit(0);
                } else
                    if (cmdstruc->background)
                        add_process(curpid);
            }
            if (cmdstruc->pipe) {
                close(fd[1]);
                execute(cmdstruc->pipe, fd[0]);
                close(fd[0]);
            }
            if (!cmdstruc->background)
                waitpid(curpid, &status, 0);
            while(cmdstruc->next) {
                if (cmdstruc->and && !cmdstruc->background) {
                    if (WIFEXITED(status)) {
                        if (!WEXITSTATUS(status)) {
                            cmdstruc->and = 0;
                            continue;
                        } else {
                            cmdstruc=cmdstruc->next;
                            continue;
                        }
                    }
                } else
                    if (cmdstruc->or && !cmdstruc->background) {
                        if (WIFEXITED(status)) {
                            if (WEXITSTATUS(status)) {
                                cmdstruc->or = 0;
                                continue;
                            } else {
                                cmdstruc = cmdstruc->next;
                                continue;
                            }
                        }
                    } else {
                        execute(cmdstruc->next, 0);
                        break;
                    }
            }
        }
    } else
        error("Error NULL command", 1);

    return 0;
}

void free_memory(struct cmd *cmdstruc) {
    char **tmp;

    if (cmdstruc) {
        free(cmdstruc->input_file);
        free(cmdstruc->output_file);
        tmp = cmdstruc->argv;
        if (tmp) {
            while (*tmp)
                free(*(tmp++));
        }
        free(cmdstruc->argv);
        free_memory(cmdstruc->next);
        free_memory(cmdstruc->pipe);
        free_memory(cmdstruc->subcmd);
        free(cmdstruc);
    }
}


char *readl() {
    char c, *buffer = NULL;
    int i = 0, quote1 = 0, quote2 = 0;
    while ((c = getchar()) != EOF) {
        if (c == '\\') {
            if (buffer[i-1] == '\\') {
                continue;
            }
        }
        if (c == ' ') {
            if (buffer != NULL)
                if (buffer[i-1] == '\"' && buffer[i-2] == '\\' && !quote2) {
                    quote1 = ++quote1 % 2;
                }
        }
        if (c == '#' && !quote1 && !quote2) while ((c = getchar()) != '\n');
        if (c == '\"' && quote2 % 2 == 0) {
            if (buffer[i-1] != '\\' || !quote1) {
                quote1 = ++quote1 % 2;
            }
        }
        if (c == '\'' && quote1 % 2 == 0) {
            if (buffer[i-1] != '\\' || !quote2) {
                quote2 = ++quote2 % 2;
            }
        }

        if (c == '\n' && !quote1 && !quote2) {
            if (c == '\n' && buffer != NULL && buffer[i-1] == '\\') {
                printf("%s", INVITE);
                --i;
                continue;
            }
            i += 2;
            buffer = (char *)realloc(buffer, i * sizeof(char));
            buffer[i-2] = '\n';
            buffer[i-1] = '\0';
            return buffer;
        }
        if (c == '\n' && quote1) {
            buffer = (char *)realloc(buffer, ++i * sizeof(char));
            buffer[i-1] = '$';
            continue;
        }
        buffer = (char *)realloc(buffer, ++i * sizeof(char));
        buffer[i-1] = c;
    }

    return NULL;

}



int main(int argc, char **argv) {
    while (1) {
        do {
            setjmp(point);
            signal(SIGINT, SIG_IGN);
            token[0] = 0;
            cursor = 0;
            print_list();
            printf("%s", INVITE);
            char *str = readl();

            if (!str)
                exit(0);
            strncpy(buffer, str, strlen(str));
            get_token();
        } while (!(head = parse()));
        execute(head, 0);
        free_memory(head);
    }

    return 0;
}
