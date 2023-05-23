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
#include <sys/stat.h>
#include <time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/file.h>

#define pack_size 1024
#define fn_size 200

char *find_file(char *filename, int ts, char *chunk);
void ch(int sig);

int main(int argc, char **argv) {
	if (argc != 3) {
		printf("Usage: %s <directory> <port number>\n", argv[0]);
		exit(0);
	}
	
	//CTRL-C
	signal(SIGINT, ch);
	
	int port, sfd, afd, selflen, clientlen;
	struct sockaddr_in self, client;
	
	//Make dfs directory
	if ((mkdirat(AT_FDCWD, argv[1], 0770)) == -1) {
		//directory already exists
	}
	
	//Address info
	port = atoi(argv[2]);
	self.sin_family = AF_INET;
  	self.sin_addr.s_addr = htonl(INADDR_ANY);
  	self.sin_port = htons((unsigned short)port);
  	selflen = sizeof(self);
	clientlen = sizeof(struct sockaddr_storage);
	
	//Set up connection
	if ((sfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		fprintf(stderr, "Error on socket creation");
		exit(1);
	}
	
	if (bind(sfd, (struct sockaddr *) &self, selflen) < 0) {
		fprintf(stderr, "Error on bind");
		exit(1);
	}
	
	if (listen(sfd, 10) < 0) {
		fprintf(stderr, "Error on listen");
		exit(1);
	}
	
	while (1) {
		//Accept the connection
		if ((afd = accept(sfd, (struct sockaddr *) &client, &clientlen)) < 0) {
			fprintf(stderr, "ERROR on accept");
			exit(1);
		}
		
		if (fork() == 0) {
			break;
		}
		else
			close(afd);
	}
	//From beejs to reuse port
	int optval = 1;
	setsockopt(afd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
	
	char packet[pack_size], command[5], filename[fn_size], timestamp[fn_size], *line = NULL, request[fn_size], *temp;
	int brec, bsent, total, bytecounter = 0, size, fd;
	long targetbytes;
	FILE *file;
	
	//Clear buffers
	bzero(packet, pack_size);
	bzero(command, 5);
	bzero(filename, fn_size);
	bzero(request, fn_size);
	//receive request from client
	if ((brec = recv(afd, packet, pack_size, 0)) < 0) {
		fprintf(stderr, "error in recv");
		return 1;
	}
	
	if (packet[0] == 'a') {
		bzero(packet, pack_size);
		strncpy(packet, "ls -l ", 6);
		strncat(packet, argv[1], fn_size);
		strncat(packet, " | grep '^d' | awk -F' ' '{print $9}'", fn_size);
		file = popen(packet, "r");
		bzero(packet, pack_size);
		size_t len;
		
		while (getline(&line, &len, file) != -1) {
			if ((bsent = send(afd, line, strlen(line), 0)) < 0) {
				fprintf(stderr, "error in send");
			}
		}
		if ((bsent = send(afd, "/\n", 1, 0)) < 0) {
			fprintf(stderr, "error in send");
		}
		
		pclose(file);
	} else if (packet[0] == 'g') {
		line - strtok(packet, " ");
		line = strtok(NULL, " ");
		strncpy(filename, argv[1], fn_size);
		strncat(filename, "/", 2);
		strncat(filename, line, fn_size-1);
		
		//send size of file
		if (!(file = fopen(filename, "r"))) {
			fprintf(stderr, "can't open file\n");
			//send error back to client
		}
		fseek(file, 0, 2);
		targetbytes = ftell(file);
		fseek(file, 0, 0);
		bzero(packet, pack_size);
		sprintf(packet, "%ld", targetbytes);
		strncat(packet, "\r\n\r\n", 5);
		if ((bsent = send(afd, packet, strlen(packet), 0)) < 0) {
			fprintf(stderr, "error in send");
			//send error to client
		}
		
		//send file contents to client
		bzero(packet, pack_size);
		while (fread(packet, 1, pack_size, file)) {
			total = 0;
			if (targetbytes - bytecounter < pack_size) {
				while (total < targetbytes - bytecounter) {
					if ((bsent = send(afd, packet + total, targetbytes - bytecounter - total, 0)) < 0) {
						fprintf(stderr, "Error in sendto");
						return 1;
					}
					total += bsent;
				}
			} else {
				while (total < pack_size) {
					if ((bsent = send(afd, packet + total, pack_size - total, 0)) < 0) {
						fprintf(stderr, "Error in sendto");
						return 1;
					}
					total += bsent;
				}
			}
			
			bytecounter += total;
			if (bytecounter >= targetbytes) { break; }
			
			//Clear buf
			bzero(packet, pack_size);
		}
	} else if (packet[0] == 'p') {
		//receive request from client
		/*
			loop until \n, then do packet +that in recv
		*/
		if ((brec += recv(afd, packet+brec, pack_size-brec, 0)) < 0) {
			fprintf(stderr, "error in recv");
			return 1;
		}
		//receive size of file from client
		size = 0;
		for (int i = 0; i < brec; i++) {
			if (packet[i] == '\r') {
				if (packet[i+3] == '\n') {
					total = i + 4;
					break;
				} else {
					bsent = i;
				}
			}
			if (packet[i] == ' ') {
				packet[i] = '\0';
			}
		}
		targetbytes = atoi(packet+bsent+2);
		
		strncpy(filename, argv[1], fn_size-30);
		strncat(filename, "/", 2);
		strncat(filename, packet+4, fn_size-1);
		
		//mkdir with filename
		if (mkdir(filename, 0770) == -1) {}
		
		//concat timestamp to filename
		strncat(filename, "/", 2);
		strncat(filename, packet+4+strlen(packet+4)+1, bsent-4-strlen(packet+4)-1);

		//open filename and lock
		file = fopen(filename, "w+");
		fd = fileno(file);
		flock(fd, LOCK_EX);
		
		if (packet+total) {
			fwrite(packet+total, 1, brec-total, file);
		}
		
		//get file content
		bytecounter = brec-total;
		while (bytecounter < targetbytes) {
			bzero(packet, pack_size);
			if ((brec = recv(afd, packet, pack_size, 0)) < 0) {
				fprintf(stderr, "error in recv");
				return 1;
			}
			
			//write contents to file
			fwrite(packet, 1, brec, file);
			
			bytecounter += brec;
		}
		
		//unlock and close file
		flock(fd, LOCK_UN);
		fclose(file);
	} else if (packet[0] =='l') {
		bzero(filename, fn_size);
		bzero(request, fn_size);
		line = strtok(packet, " ");
		line = strtok(NULL, " ");
		strncpy(request, argv[1], fn_size);
		strncat(request, "/", 2);
		strncat(request, line, fn_size);
		strncpy(filename, line, fn_size);
		line = strtok(NULL, " ");
		strncpy(timestamp, line, fn_size);
		
		/*
			if first file won't open, call find_file
			return 0 and filename in packet
		*/
		
		//request has full name, filename has no chunk #
		line = strtok(timestamp, "_");
		line = strtok(NULL, "_");
		strncat(request, "_", 2);
		strncat(request, line, fn_size);
		bzero(request+strlen(request)-2, fn_size-strlen(request)+2);
		if ((file = fopen(request, "r"))) {
			bzero(packet, pack_size);
			sprintf(packet, "%i", 0);
			strncat(packet+strlen(packet)+1, filename, fn_size);
			//send packet back, just a number
			size = strlen(packet)+strlen(packet+strlen(packet)+1)+1;
			if ((bsent = send(afd, packet, size, 0)) < 0) {
				fprintf(stderr, "Error in sendto");
				return 1;
			}
			fclose(file);
		} else {
			bzero(request, fn_size);
			strncpy(request, argv[1], fn_size);
			strncat(request, "/", 2);
			strncat(request, filename, fn_size);
			if ((line = find_file(request, atoi(timestamp), line))) {
				bzero(packet, pack_size);
				sprintf(packet, "%i", 0);
				strncat(filename, "/", 2);
				strncat(filename, line, fn_size);
				strncat(packet+strlen(packet)+1, filename, fn_size);
				//send packet back, just a number
				size = strlen(packet)+strlen(packet+strlen(packet)+1)+1;
				if ((bsent = send(afd, packet, size, 0)) < 0) {
					fprintf(stderr, "Error in sendto");
					return 1;
				}
				free(line);
			} else {
				bzero(packet, pack_size);
				sprintf(packet, "%i", -1);
				//send packet back, just a number
				if ((bsent = send(afd, packet, strlen(packet), 0)) < 0) {
					fprintf(stderr, "Error in sendto");
					return 1;
				}
			}
		}
		
	} else {
		//not a command
	}
	
	//on list, just try to open file and return 0 on success
	close(afd);
	exit(0);
}

// ls on filename directory, return file of closest timestamp, return 1 on success
char *find_file(char *filename, int ts, char *chunk) {
	char buffer[pack_size],  *line = NULL, *temp = NULL, *newname;
	short i;
	size_t len;
	FILE *pshell;
	//int hits = 1700000000;
	//get name of most recent file
	bzero(buffer, pack_size);
	strncpy(buffer, "ls -l ", 6);
	strncat(buffer, filename, fn_size);
	strncat(buffer, " --sort=time --time-style=+%s | awk -F' ' '{print $6,$7}' | grep '_", fn_size);
	strncat(buffer, chunk, fn_size);
	strncat(buffer, "'", 2);
	pshell = popen(buffer, "r");
	
	//store filename in buffer
	//getline, keep going til first timestamp that is too old
	while (getline(&line, &len, pshell) != -1) {
		if (strncmp("ls: ", line, 4) == 0) {
			break;
		}
		if (strlen(line) > 5) {
			temp = strtok(line, " ");
			if (atoi(temp) < ts) {
				//return new filename in buffer and send back in packet
				newname = malloc(fn_size*sizeof(char));
				bzero(newname, fn_size);
				temp = strtok(NULL, " ");
				//loop until _
				for (i = 0; i < strlen(line); i++) {
					if (temp[i] == '_') {
						break;
					}
				}
				strncat(newname, temp, i);
				pclose(pshell);
				return newname;
			}
		}
	}
	pclose(pshell);
	return NULL;
}

void ch(int sig) {
	exit(0);
}
