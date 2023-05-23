// Brady Kuzinski
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h> 
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/file.h>
//#include <openssl/md5.h>

#define pack_size 1024
#define fn_size 100
#define conf_size 200
#define addrs_size 100
#define ports_size 100
#define section_size 25

void ch(int sig);
void hash_filename(char *filename, int amnt_servers, int **chunk);
int conn_server(char *command, char *filename, char *dfs_address, int dfs_port, FILE *dfile, long timestamp, int chunk_size, int chunk_num);
char *list_all(char *address, int port, int sv_count);

int main(int argc, char **argv) {
	int i;
	for (i = 0; i < strlen(argv[1]); i++) {
		if (argv[1][i] < 91) {
			argv[1][i] += 32;
		}
	}
	
	if (argc == 2 && strncmp(argv[1], "list", 4) == 0) {
		//allow list <> <>
	} else if (argc < 3) {
		printf("Usage: %s <command> [filename] ... [filename]\n", argv[0]);
		exit(1);
	}
	
	//CTRL-C
	signal(SIGINT, ch);
	
	//put command in lowercase, checking size first
	if (strlen(argv[1]) > 4) {
		printf("Invalid command\n");
		exit(1);
	}
	
	if ((strncmp(argv[1], "get", 3) != 0) && (strncmp(argv[1], "put", 3) != 0) && (strncmp(argv[1], "list", 4) != 0)) {
		printf("Invalid command\n");
		exit(1);
	}
	
	//char array to store info from ~/dfc.conf
	char server_conf[conf_size], addresses[addrs_size], ports[ports_size], *filename = malloc(fn_size*sizeof(char)), *line = NULL, *temp, *backup;
	int arg_file = 2, server_count = 0, size, chunk_num = 1, filechunk_count = 0, j, fd, chunk_size, total_size;
	long file_ts, targetbytes;
	size_t len;
	FILE *file;
	
	//get info from ~/dfc.conf
	bzero(server_conf, conf_size);
	strncpy(server_conf, getenv("HOME"), conf_size/2);
	strncat(server_conf, "/dfc.conf", 10);
	if (!(file = fopen(server_conf, "r"))) {
		fprintf(stderr, "Can't find ~/dfc.conf\n");
		return 1;
	}
	
	//store servers and ports in order
	bzero(server_conf, conf_size);
	while (getline(&line, &len, file) != -1) {
		server_count++;
		temp = strtok(line, " ");
		temp = strtok(NULL, " ");
		size = atoi(temp + 3);
		temp = strtok(NULL, " ");
		strncat(server_conf+(2*section_size*(size - 1)), temp, 2*section_size-1);
	}
	fclose(file);
	
	//separate address and port
	for (i = 0; i < conf_size; i++) {
		if (server_conf[i] == ':' || server_conf[i] == '\n') {
			server_conf[i] = '\0';
		}
	}
	
	//Store addresses and ports in different arrays and malloc 2d array for chunk locations
	bzero(addresses, addrs_size);
	bzero(ports, ports_size);
	int **dfs = malloc(server_count*sizeof(int));
	for (i = 0; i < server_count; i++) {
		dfs[i] = malloc(2*sizeof(int));
		strncat(addresses+(i*section_size), server_conf+(i*section_size*2), section_size-1);
		strncat(ports+(i*section_size), server_conf+(i*section_size*2)+strlen(server_conf+(i*section_size*2))+1, section_size-1);
	}
	
	//If get or list, call list
	if ((argc == 2) && (argv[1][0] == 'l')) {
		filechunk_count = 0;
		for (i = 0; i < server_count; i++) {
			line = list_all(addresses+(i*section_size), atoi(ports+(i*section_size)), server_count);
			if (line && line[0] != '\0') {
				break;
			}
			free(line);
			filechunk_count++;
		}
		if (filechunk_count == server_count) {
			printf("Can't connect to any servers\n");
		}
		
		backup = line;
		if (line && line[0] != '\0') {
			temp = strtok(line, "\n");
			while (temp) {
				bzero(filename, fn_size);
				strncpy(filename, temp, fn_size);
				chunk_num = 1;
				filechunk_count = 0;
				hash_filename(filename, server_count, dfs);
				
				//timestamp for file
				file_ts = time(NULL);
				while (chunk_num <= server_count) {
					for (j = 0; j < server_count; j++) {
						for (i = 0; i < 2; i++) {
							if (dfs[j][i] == chunk_num) {
								if (conn_server("list", filename, addresses+(j*section_size), atoi(ports+(j*section_size)), NULL, file_ts, 0, chunk_num) == 0) {
									filechunk_count++;
									j = server_count+1;
									break;
								}
							}
						}
					}
					chunk_num++;
				}
				printf("%s", temp);
				if (server_count != filechunk_count) {
					printf(" [incomplete]");
				}
				printf("\n");
				temp = strtok(NULL, "\n");
			}
			free(backup);
		}
	} else if ((argv[1][0] == 'g') || (argv[1][0] == 'l')) {
		while (arg_file < argc) {
			//call list to see if file can be constructed
			bzero(filename, fn_size);
			strncpy(filename, argv[arg_file], fn_size);
			chunk_num = 1;
			filechunk_count = 0;
			hash_filename(filename, server_count, dfs);
			
			//timestamp for file
			file_ts = time(NULL);
			while (chunk_num <= server_count) {
				for (j = 0; j < server_count; j++) {
					for (i = 0; i < 2; i++) {
						if (dfs[j][i] == chunk_num) {
							if (conn_server("list", filename, addresses+(j*section_size), atoi(ports+(j*section_size)), NULL, file_ts, 0, chunk_num) == 0) {
								filechunk_count++;
								j = server_count+1;
								break;
							}
						}
					}
				}
				chunk_num++;
			}
			
			//verify file can be constructed
			if (filechunk_count != server_count) {
				if (argv[1][0] == 'g') {
					printf("%s is incomplete\n", argv[arg_file]);
				} else {
					printf("%s [incomplete]\n", argv[arg_file]);
				}
				arg_file++;
				continue;
			} else {
				if (argv[1][0] == 'l') {
					printf("%s\n", argv[arg_file]);
					arg_file++;
					continue;
				}
			}
			
			//open file and lock
			file = fopen(argv[arg_file], "w+");
			fd = fileno(file);
			flock(fd, LOCK_EX);
			
			//get file chunks from servers
			chunk_num = 1;
			filechunk_count = 0;
			while (chunk_num <= server_count) {
				filename[strlen(filename)] = '_';
				sprintf(filename+strlen(filename), "%i", chunk_num);
				for (j = 0; j < server_count; j++) {
					for (i = 0; i < 2; i++) {
						if (dfs[j][i] == chunk_num) {
							if (conn_server("get", filename, addresses+(j*section_size), atoi(ports+(j*section_size)), file, file_ts, 0, chunk_num) == 0) {
								filechunk_count++;
								j = server_count;
								break;
							}
						}
					}
				}
				chunk_num++;
				//get rid of end chunk number
				for (i = strlen(filename)-1; i >= 0; i--) {
					if (filename[i] == '_') {
						bzero(filename+i, fn_size-i);
						break;
					}
				}
			}
			
			//unlock file and continue
			flock(fd, LOCK_UN);
			fclose(file);
			arg_file++;
		}
	} else {
		//put
		while (arg_file < argc) {
			bzero(filename, fn_size);
			strncpy(filename, argv[arg_file], fn_size);
			chunk_num = 1;
			filechunk_count = 0;
			hash_filename(filename, server_count, dfs);
			
			//Only need to loop through server conf file addresses
			filechunk_count = 0;
			for (i = 0; i < server_count; i++) {
				if (conn_server("connect-only", filename, addresses+(i*section_size), atoi(ports+(i*section_size)), NULL, file_ts, 0, 0) == 0) {
					filechunk_count++;
				}
			}
			
			//make sure we can connect to the servers
			if (filechunk_count != server_count) {
				printf("%s put failed\n", argv[arg_file]);
				arg_file++;
				continue;
			}
			
			//store filename in buffer
			bzero(filename, fn_size);
			strncpy(filename, argv[arg_file], fn_size);
			
			//timestamp for file
			file_ts = time(NULL);
			
			//open file and lock
			if (!(file = fopen(filename, "r"))) {
				printf("Can't open file\n");
				arg_file++;
				continue;
			}
			
			//get size of file
			fseek(file, 0, 2);
			targetbytes = ftell(file);
			fseek(file, 0, 0);
			
			//increment problem
			total_size = 0;
			chunk_size = targetbytes / server_count;
			
			/*
				divide by server count and leave remainder to last server
			*/
			
			//get file chunks from servers
			chunk_num = 1;
			filechunk_count = 0;
			total_size = 0;
			while (chunk_num <= server_count) {
				for (j = 0; j < server_count; j++) {
					for (i = 0; i < 2; i++) {
						if (dfs[j][i] == chunk_num) {
							if (conn_server("put", filename, addresses+(j*section_size), atoi(ports+(j*section_size)), file, file_ts, chunk_size, chunk_num) == 0) {
								//filechunk_count++;
							}
						}
						fseek(file, total_size, 0);
					}
				}
				total_size += chunk_size;
				fseek(file, total_size, 0);
				if (chunk_num == server_count-1) {
					chunk_size = targetbytes - total_size;
				}
				chunk_num++;
			}
			
			//unlock file and continue
			fclose(file);
			arg_file++;
		}
	}

	return 0;
}

//SIGINT handler
void ch(int sig) {
	exit(0);
}

//hash function for filename and return chunk locations
/*
	djb2
	cse.yorku.ca/~oz/hash.html
*/
void hash_filename(char *filename, int amnt_servers, int **chunk) {
	char packet[pack_size];
	bzero(packet, pack_size);
	strncpy(packet, filename, fn_size);
	unsigned long hash = 5381;
	int c, n;
	
	while ((c = (*packet)++))
		hash = ((hash << 5) + hash) + c;

	n = hash % amnt_servers;
    
	if (n == 0) {
		chunk[0][0] = 1;
		chunk[0][1] = 2;
		chunk[1][0] = 2;
		chunk[1][1] = 3;
		chunk[2][0] = 3;
		chunk[2][1] = 4;
		chunk[3][0] = 4;
		chunk[3][1] = 1;
	} else if (n == 1) {
		chunk[0][0] = 4;
		chunk[0][1] = 1;
		chunk[1][0] = 1;
		chunk[1][1] = 2;
		chunk[2][0] = 2;
		chunk[2][1] = 3;
		chunk[3][0] = 3;
		chunk[3][1] = 4;
	} else if (n == 2) {
		chunk[0][0] = 3;
		chunk[0][1] = 4;
		chunk[1][0] = 4;
		chunk[1][1] = 1;
		chunk[2][0] = 1;
		chunk[2][1] = 2;
		chunk[3][0] = 2;
		chunk[3][1] = 3;
	} else {
		chunk[0][0] = 2;
		chunk[0][1] = 3;
		chunk[1][0] = 3;
		chunk[1][1] = 4;
		chunk[2][0] = 4;
		chunk[2][1] = 1;
		chunk[3][0] = 1;
		chunk[3][1] = 2;
	}
}



int conn_server(char *command, char *filename, char *dfs_address, int dfs_port, FILE *dfile, long timestamp, int chunk_size, int chunk_num) {
	char packet[pack_size];
	int sfd, serverlen, brec, bsent, bytecounter, rval;
	long targetbytes;
	struct sockaddr_in server;
	struct timeval to;
	
	//create socket for server
	if ((sfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		fprintf(stderr, "Error on socket creation");
		return 1;
	}
	
	//set socket timeout
	to.tv_sec = 1;
	setsockopt(sfd, SOL_SOCKET, SO_RCVTIMEO, &to, sizeof(struct timeval));
	
	//server info
	bzero((char *) &server, sizeof(server));
	server.sin_family = AF_INET;
	server.sin_addr.s_addr = inet_addr(dfs_address);
	server.sin_port = htons(dfs_port);
	serverlen = sizeof(server);
	
	//connect to server
	if (connect(sfd, (struct sockaddr *)&server, serverlen) < 0) {
		//fprintf(stderr, "Can't connect to server\n");
		return 1;
	}
	
	//if command is 'connect-only', close and return fine for put
	if (command[0] == 'c') {
		if ((bsent = send(sfd, "null filename timestamp", pack_size, 0)) < 0) {
			fprintf(stderr, "Error in sendto");
			return 1;
		}
		close(sfd);
		return 0;
	}
	
	//send command, filename, timestamp
	bzero(packet, pack_size);
	strncpy(packet, command, 5);
	strncat(packet, " ", 2);
	strncat(packet, filename, strlen(filename));
	strncat(packet, " ", 2);
	sprintf(packet+strlen(packet), "%li", timestamp);
	strncat(packet, "_", 2);
	sprintf(packet+strlen(packet), "%i", chunk_num);
	strncat(packet, "\r\n", 5);
	if ((bsent = send(sfd, packet, strlen(packet), 0)) < 0) {
		fprintf(stderr, "error in send");
		return 1;
	}
	
	if (command[0] == 'g') {
		//response from server is amount of bytes to be received
		bzero(packet, pack_size);
		if ((brec = recv(sfd, packet, pack_size, 0)) < 0) {
			fprintf(stderr, "error in recv");
			return 1;
		}
		targetbytes = atoi(packet);
		
		for (int i = 0; i < strlen(packet); i++) {
			if (packet[i] =='\r' && packet[i+3] == '\n') {
				fwrite(packet+i+4, 1, brec-i-4, dfile);
				bytecounter = brec-i-4;
				break;
			}
		}
		
		//write bytes to file
		while (bytecounter < targetbytes) {
			bzero(packet, pack_size);
			if ((brec = recv(sfd, packet, brec, 0)) < 0) {
				fprintf(stderr, "error in recv");
				return 1;
			}
			
			//write contents to file
			fwrite(packet, 1, brec, dfile);
			
			bytecounter += brec;
		}
	} else if (command[0] == 'p') {
		//send file size to server
		targetbytes = chunk_size;
		bzero(packet, pack_size);
		sprintf(packet, "%li", targetbytes);
		strncat(packet, "\r\n\r\n", 5);
		if ((bsent = send(sfd, packet, strlen(packet), 0)) < 0) {
			fprintf(stderr, "Error in sendto");
			return 1;
		}
	
		//send file contents to server
		bytecounter = 0;
		bzero(packet, pack_size);
		while (fread(packet, 1, pack_size, dfile)) {
			if (targetbytes - bytecounter > pack_size) {
				if ((bsent = send(sfd, packet, pack_size, 0)) < 0) {
					fprintf(stderr, "Error in sendto");
					return 1;
				}
			} else {
				if ((bsent = send(sfd, packet, targetbytes-bytecounter, 0)) < 0) {
					fprintf(stderr, "Error in sendto");
					return 1;
				}
			}
			bytecounter += bsent;
			
			if (bytecounter >= targetbytes) {
				break; 
			}
			
			//clear buf
			bzero(packet, pack_size);
		}
	} else {
		bzero(packet, pack_size);
		if ((brec = recv(sfd, packet, pack_size, 0)) < 0) {
			fprintf(stderr, "error in recv");
			return 1;
		}
		//return packet sends 0 and filename if file exists
		rval = atoi(packet);
		if (rval == 0) {
			bzero(filename, fn_size);
			strncpy(filename, packet+strlen(packet)+1, fn_size);
		}
		close(sfd);
		return rval;
	}
	
	close(sfd);
	return 0;
}

char *list_all(char *address, int port, int sv_count) {
	int sfd, serverlen, brec, bsent, size;
	struct sockaddr_in server;
	struct timeval to;
	
	//create socket for server
	if ((sfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		fprintf(stderr, "Error on socket creation");
		return NULL;
	}
	
	//set socket timeout
	to.tv_sec = 5;
	setsockopt(sfd, SOL_SOCKET, SO_RCVTIMEO, &to, sizeof(struct timeval));
	
	//server info
	bzero((char *) &server, sizeof(server));
	server.sin_family = AF_INET;
	server.sin_addr.s_addr = inet_addr(address);
	server.sin_port = htons(port);
	serverlen = sizeof(server);
	
	//connect to server
	if (connect(sfd, (struct sockaddr *)&server, serverlen) < 0) {
		//fprintf(stderr, "Can't connect to server\n");
		return NULL;
	}
	
	char *ls_return = NULL, *temp, packet[pack_size];
	if ((bsent = send(sfd, "all", 3, 0)) < 0) {
		fprintf(stderr, "Error in sendto");
		return NULL;
	}
	
	size = 2*fn_size*sizeof(char);
	ls_return = malloc(size);
	bzero(ls_return, size);
	//Sorry in a rush, just needed to slow this one down rq
	while (1) {
		bzero(packet, pack_size);
		if ((brec = recv(sfd, packet, pack_size, 0)) < 0) {
			fprintf(stderr, "error in recv");
			return NULL;
		}
		temp = strtok(packet, "\n");
		while (temp) {
			if (strncmp(packet, "/", 1) == 0) {
				size = -1;
				break;
			}
			strncat(ls_return, temp, strlen(temp));
			strncat(ls_return, "\n", 2);
			temp = strtok(NULL, "\n");
		}
		if (size == -1) {
			break;
		}
	}
	close(sfd);
	return ls_return;
}
