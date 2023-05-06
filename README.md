# Backend Specific Instructions
## Download large files:
***Put these three files into the /data folder***  
- `englishwords.txt`: https://drive.google.com/file/d/1SaXCfJu-zUjSiWVWMDbGxpu3QhbFaZKc/view?usp=share_link  
- `glove.6B.50d.txt`: https://drive.google.com/file/d/1ZPHgJB1L90dNspy085HD0L7UWjgRYNua/view?usp=share_link  
- `stopwords.txt`: https://drive.google.com/file/d/1VpvjO7y1P5LA-gbdQunnQhzqbWIZQ9t2/view?usp=share_link  

***Download six workers folders from Google Drive and only leave id, urlpages.table, index.table, and pageranks.table in the folders ***
- `Google Drive Link`: https://drive.google.com/drive/u/1/folders/1ZJzV18o8drNQDO7vvCZWd3MWLW1HS1Wk

***Note***: Each worker folder need to be stay on the root directory of /NetNinjas-ssh.

## How to start the backend server
To start the server, first start the KVS master and two KVS workers like this:

1. java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Master 8000 
2. java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8001 worker1 34.231.82.155:8000
3. java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8002 worker2 34.231.82.155:8000
4. java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8003 worker3 34.231.82.155:8000
5. java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8004 worker4 34.231.82.155:8000
6. java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8005 worker5 34.231.82.155:8000
7. java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8006 worker6 34.231.82.155:8000

Then start the backend server:
- ***java -cp lib/kvs.jar:lib/webserver.jar:lib/gson.jar:lib/ranking.jar src/cis5550/jobs/SearchApi.java 34.231.82.155:8000 8080***

Note that the last two arguments correspond to **KVSClient address** and **Backend port number**, respectively.  

## Supported routes
### #Route 1
***`get("/")`***

This route returns the main page of Nenninjas search engine

*Request Params*:  
- None  

*Query Params*:
- None

*Example Input*:  
```
http://34.231.82.155:8080/
```

*Example Response*: 
A nice Netninjas search page

### #Route 2
***`get("/search") `*** 

This search API accepts a query string as input and return a list of search results that match the query.

*Request Params*:  
- None  

*Query Params*:
- **query**: the user input search query passed from the frontend
- **pageSize**: the number of results on a single page
- **pageNum**: the current page number

*Example Input*:  
```
http://34.231.82.155:8080/search?query=apple?&pageSize=10&pageNum=1
```

*Example Response*: 
```json 
{ "count": [ { "url": "1591", "title": "", "snippet": "" } ], "results": [ { "url": "https://music.apple.com:443/mz/browse", "title": " apple music", "snippet": "apple music african davido stays focused on the bag on unavailable...." }, { "url": "https://music.apple.com:443/lr/browse", "title": " apple music", "snippet": "apple music african a seminal voice in abujas hiphop scene comes to the fore...." }]}
```

### Route 3
***`get("/synonym")`***  

This route will return sentences that are similar to input query and synonym words of each non-stop words

*Request Params*:  
- None  

*Query Params*:  
- **query**: the user input search query passed from the frontend

*Example Input*:
```  
http://34.231.82.155:8080/synonym?query=pizza%20near%20Sydney_help
```

*Example Response*:  
```json
{ "results": { "similarSentences": [ "sandwich near sydney help", "pizza nearby sydney help", "pizza near melbourne help", "pizza near sydney helping" ], "storedSynonyms": [ { "rootWord": "help", "similarWords": [ "helping", "bring", "need", "take", "helps" ] }, { "rootWord": "sydney", "similarWords": [ "melbourne", "adelaide", "brisbane", "perth", "auckland" ] }, { "rootWord": "near", "similarWords": [ "nearby", "town", "area", "outskirts", "northeast" ] }, { "rootWord": "pizza", "similarWords": [ "sandwich", "sandwiches", "snack", "bakery", "fries" ] } ] } }  
```

### Route 4
***`get("/autocomplete")`***   
This API accepts a query string as input and return a list of suggested search terms based on the last word of the search string.

*Request Params*:  
- None

*Query Params*:
- **query**: the user input search query passed from the frontend

*Example Input*:  
```
http://34.231.82.155:8080/autocomplete?query=appl
```

*Example Response*:  
```json
{ "suggestions": [ { "term": "appl", "count": 0 }, { "term": "applause", "count": 0 }, { "term": "applauses", "count": 0 }, { "term": "applausive", "count": 0 }, { "term": "applausively", "count": 0 }, { "term": "applaud", "count": 0 }, { "term": "applaudable", "count": 0 }, { "term": "applaudably", "count": 0 }, { "term": "applauds", "count": 0 }, { "term": "applauder", "count": 0 } ] }
```

### Route 5
***`get("/emptyquery")`***  
This route can be called when the input query is null. The response will be up to 5 most recent search histories.

*Request Params*:  
- None

*Query Params*:  
- None

*Example Input*:  
```
http://34.231.82.155:8080/emptyquery
```

*Example Response*:  
```json
{ "results": [ { "url": "https://en.wikipedia.org:443/wiki/2023_in_hip_hop_music"}, { "url": "https://en.wikipedia.org:443/wiki/Taylor_Swift_(album)"} ] } 
```

## EC2
### Git setup
To install Git on an EC2 instance running Amazon Linux 2, you can use the following command:
```bash
sudo yum install git -y
git --version
```

To set up Git on your machine, follow these steps:
1. Configure Git: After installing Git, you need to configure your Git username and email address using the following commands:

```bash
git config --global user.name "Your Name"
git config --global user.email "youremail@example.com"
```
Replace "Your Name" and "youremail@example.com" with your actual name and email address.

2. Check your Git configuration: You can check your Git configuration using the following command:
```bash
git config --list
```

3. Set up SSH key: If you plan to use Git with SSH, you need to set up an SSH key. To do this, follow these steps:
  a. Generate an SSH key using the following command:
  ```bash
    ssh-keygen -t rsa -b 4096 -C "youremail@example.com"
  ```
  Replace "youremail@example.com" with your actual email address.

  b. Copy the SSH key to your clipboard using the following command:
  ```bash
    pbcopy < ~/.ssh/id_rsa.pub
  ```
  If you are on a Linux machine, use the following command instead:
  ```bash
    cat ~/.ssh/id_rsa.pub
  ```
 
  c. Add the SSH key to your Git account: Log in to your Git account and go to your account settings. Click on "SSH and GPG keys" and then click on "New SSH key". Paste the SSH  key that you copied in the previous step into the "Key" field and give the key a title.
  
### gh github CLI
To install GitHub CLI (gh) on Linux EC2, you can follow these steps:

1. Open your SSH terminal and update the package list by running the following command:
```bash
sudo yum update
```

2. Install the required dependencies by running the following command:
```bash
sudo yum install -y ca-certificates git
```

3. Download the latest version of the gh binary from the GitHub CLI releases page by running the following command:
```bash
curl -L -o gh.tar.gz https://github.com/cli/cli/releases/download/v2.1.0/gh_2.1.0_linux_amd64.tar.gz
```
Note: You can replace the version number with the latest version available on the releases page.

4. Extract the gh binary from the tarball by running the following command:

```bash
tar -xzf gh.tar.gz gh_2.1.0_linux_amd64/bin/gh --strip-components=1
```
Note: Replace the version number and path to the gh binary in the command with the appropriate values for your system.

5. Move the gh binary to the /usr/local/bin directory by running the following command:

```bash
sudo mv gh /usr/local/bin/
```
Verify that gh is installed correctly by running the following command:

```bash
gh --version
```
This should display the version number of gh installed on your system.