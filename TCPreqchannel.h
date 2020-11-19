
#ifndef _TCPreqchannel_H_
#define _TCPreqchannel_H_

#include "common.h"
#include <sys/socket.h>
#include <netdb.h>

class TCPRequestChannel
{
public:
	enum Mode {READ_MODE, WRITE_MODE};
	
private:
	int sockfd;
	
public:
	TCPRequestChannel(const string host, const string port);

	TCPRequestChannel(int);
	

	~TCPRequestChannel();


	int cread(void* msgbuf, int bufcapacity);
	
	
	int cwrite(void *msgbuf , int msglen);
	
	 
	int getfd();

};

#endif
