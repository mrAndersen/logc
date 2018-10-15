`docker pull mrandersen7/logc:0.1`
`docker run -d --name=logc --net-alias=logc -p 914:914/udp -v /logc.ini:/usr/local/logc/logc.ini mrandersen7/logc:0.1`