This is a appointment project where I sending mms message to customer along with agents.
I have two workflows one is used for sending mms message and second workflow is listening to replies.

1. First workflow triggers, it sends a mms three-way message connecting customer with agent. It logs data into AWS S3 folder
2. Second workflow listens for upcoming replies in same conversation and writes data into AWS S3 folder.

First N8N workflow uses AWS lambda functions to send messages. Lambda functions are triggered in https node using function URL.
