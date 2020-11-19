#include "common.h"
#include "BoundedBuffer.h"
#include "Histogram.h"
#include "common.h"
#include "HistogramCollection.h"
#include "TCPreqchannel.h"
#include <thread>
#include <pthread.h>
#include <sys/epoll.h>
using namespace std;


void patient_thread_function(int n, int pno, BoundedBuffer* request_buffer) {
    datamsg d(pno, 0.0, 1);
    double result = 0;

    for (int i = 0; i < n; ++i) {
        request_buffer->push((char*) &d, sizeof(datamsg));
        d.seconds += 0.004;
    }
}

void setnonblocking (int fd) {
    int flags;
    if (-1 == (flags = fcntl(fd, F_GETFL, 0))) {
        flags = 0;
    }
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void event_polling_function(int n, int p, int w, int m, TCPRequestChannel** wchans, BoundedBuffer* request_buffer, HistogramCollection* hc, int mb) {
    char buf [1024];
    double result = 0;
    char recvbuf [mb];
    bool quit_recv = false;

    struct epoll_event ev;
    struct epoll_event events[w];

    unordered_map<int, int> fd_to_index;
    vector<vector<char>> state (w);

    // priming phase and adding rfd to list
    int epollfd = epoll_create1 (0);
    if (epollfd == -1) {
        EXITONERROR ("epoll_create1");
    }

    int nsent = 0, nrecv = 0;
    for (int i = 0; i < w; i++) {
        int sz = request_buffer->pop (buf, 1024);
        if (*(MESSAGE_TYPE*) buf == QUIT_MSG) {
            quit_recv = true;
            cout << "Sending quit in EVP" << endl;
            break;
        }
        wchans[i]->cwrite(buf, sz);
        state[i] = vector<char> (buf, buf + sz);
        nsent++;
        int rfd = wchans[i]->getfd();
        fcntl(rfd, F_SETFL, O_NONBLOCK);

        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = rfd;
        fd_to_index [rfd] = i;

        if (epoll_ctl (epollfd, EPOLL_CTL_ADD, rfd, &ev) == -1) {
            EXITONERROR ("epoll_ctl: listen_sock");
        }
    }

    while (true) {
        //cout << "nrecv=" << nrecv << "  nsent=" << nsent << endl;
        if (quit_recv && nsent == nrecv) {
            cout << "breaking from evp loop" << endl;
            break;
        }

        int nfds = epoll_wait(epollfd, events, w, -1);
        if (nfds == -1) {
            EXITONERROR ("epoll_wait");
        }

        for (int i = 0; i < nfds; i++) {
            int rfd = events[i].data.fd;
            int index = fd_to_index[rfd];
            int resp_sz = wchans[index]->cread(recvbuf, mb);
            nrecv++;

            // process recvbuf
            vector<char> req = state[index];
            char* request = req.data();

            MESSAGE_TYPE* m = (MESSAGE_TYPE*) request;
            
            if (*m == DATA_MSG) {
                hc->update(((datamsg*) request)->person, *(double*) recvbuf);
            } else if (*m == FILE_MSG) {
                filemsg* f = (filemsg*) request;
                string filename = (char*)(f + 1);
                int size = sizeof(filemsg) + filename.size() + 1;
                string recvname = "recv/" + filename;
                FILE* fp = fopen(recvname.c_str(), "r+");
                fseek(fp, f->offset, SEEK_SET);
                fwrite(recvbuf, 1, f->length, fp);
                fclose(fp);
            } 

            // reuse
            if (!quit_recv) {
                int req_sz = request_buffer->pop(buf, sizeof(buf));
                if (*(MESSAGE_TYPE*) buf == QUIT_MSG) {
                    quit_recv = true;
                    cout << "Got quit msg in evp" << endl; 
                } else {
                    wchans[index]->cwrite(buf, req_sz);
                    state[index] = vector<char> (buf, buf + req_sz);
                    nsent++;
                }
            }
        }
    }
}

void file_thread_function (string filename, BoundedBuffer* request_buffer, TCPRequestChannel* chan, int mb) {
    // create the file
    string recvfname = "recv/" + filename;
    char buf [1024];
    filemsg f(0, 0);
    memcpy(buf, &f, sizeof(f));
    strcpy(buf + sizeof(f), filename.c_str());
    chan->cwrite(buf, sizeof(f) + filename.size() + 1);
    __int64_t filelength;
    chan->cread(&filelength, sizeof(filelength));
    FILE* fp = fopen(recvfname.c_str(), "w");
    fseek(fp, filelength, SEEK_SET);
    fclose(fp);

    // generate filemsgs, push onto buffer
    filemsg* fm = (filemsg*) buf;
    __int64_t rem = filelength;

    while (rem > 0) {
        fm->length = min(rem, (__int64_t) mb);
        request_buffer->push(buf, sizeof(filemsg) + filename.size() + 1);
        fm->offset += fm->length;
        rem -= fm->length;
    }
}


int main(int argc, char *argv[])
{
    int n = 1000;    //default number of requests per "patient"
    int p = 10;     // number of patients [1,15]
    int w = 100;    //default number of worker threads
    int b = 20; 	// default capacity of the request buffer, you should change this default
	int m = MAX_MESSAGE; 	// default capacity of the message buffer
    srand(time_t(NULL));
    bool isfiletransfer = false;
    string filename = "10.csv";
    string host;
    string port;

    int opt = -1;
    while ((opt = getopt(argc, argv, "m:n:b:w:p:f:h:r:")) != -1) {
        switch (opt) {
            case 'm':
                m = atoi(optarg);
                break;
            case 'n':
                n = atoi(optarg);
                break;
            case 'b':
                b = atoi(optarg);
                break;
            case 'w':
                w = atoi(optarg);
                break;
            case 'p':
                p = atoi(optarg);
                break;
            case 'f':  
                filename = optarg;
                isfiletransfer = true;
                break;
            case 'h':
                host = optarg;
                break;
            case 'r':
                port = optarg;
                break;
        }
    }  
    
    // int pid = fork();
    // if (pid == 0){
	// 	// modify this to pass along m
    //     execl ("server", "server", "-m", (char*) to_string(m).c_str(), (char *)NULL);
    // }
    
	// TCPRequestChannel* chan = new TCPRequestChannel("control", TCPRequestChannel::CLIENT_SIDE);
    BoundedBuffer request_buffer(b);
	HistogramCollection hc;


    // create histograms and add to histogram collection
    for (int i = 0; i < p; i++) {
        Histogram* h = new Histogram(10, -2.0, 2.0);
        hc.add(h);
    }

    // make new worker req channels
    TCPRequestChannel** wchans = new TCPRequestChannel* [w];
    for (int i = 0; i < w; ++i) {
        wchans[i] = new TCPRequestChannel(host, port);
    }
	
    struct timeval start, end;
    gettimeofday (&start, 0);

    if (isfiletransfer) {
        thread filethread(file_thread_function, filename, &request_buffer, wchans[0], m);

        thread evp (event_polling_function, n, p, w, m, wchans, &request_buffer, &hc, m);

        filethread.join();

        MESSAGE_TYPE q = QUIT_MSG;
        request_buffer.push((char*) &q, sizeof(q));

        evp.join();


    } else {
        thread patient [p];
        for (int i = 0; i < p; ++i) {
            patient[i] = thread (patient_thread_function, n, i+1, &request_buffer);
        }

        thread evp (event_polling_function, n, p, w, m, wchans, &request_buffer, &hc, m);

        for (int i = 0; i < p; ++i) {
            patient[i].join();
        }

        MESSAGE_TYPE q = QUIT_MSG;
        request_buffer.push((char*) &q, sizeof(q));

        evp.join();
    }

    MESSAGE_TYPE q = QUIT_MSG;
    for (int i = 0; i < w; ++i) {
        wchans[i]->cwrite(&q, sizeof(MESSAGE_TYPE));
        delete wchans[i];
    }
    delete[] wchans;

    cout << "Worker threads finished" << endl; 

    gettimeofday (&end, 0);
    // print the results
	hc.print ();
    int secs = (end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)/(int) 1e6;
    int usecs = (int)(end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)%((int) 1e6);
    cout << "Took " << secs << " seconds and " << usecs << " micro seconds" << endl;

    // q = QUIT_MSG;
    // chan->cwrite ((char *) &q, sizeof (MESSAGE_TYPE));
    // cout << "All Done!!!" << endl;

    // delete chan;
    
}
