import os, signal 
import json
def process(name): 
      
    try: 
          
        # iterating through each instance of the proess 
        for line in os.popen("ps ax | grep " + name + " | grep -v grep"):  
            fields = line.split() 
              
            # extracting Process ID from the output 
            pid = fields[0]  
              
            # terminating process  
            os.kill(int(pid), signal.SIGKILL)  
        print("Process Successfully terminated") 
          
    except Exception as e:
        print (e)
        print("Error Encountered while running script") 




def main():
	flag = 0
	jsonD = json.load(open("order.json","r"))
	for keys in jsonD.keys():
		jsonT = jsonD.get(keys)
		if jsonT.get("squared","") != "true":
			flag = 1
	if flag == 0:
		process("bid")
		process("stream")


main()
