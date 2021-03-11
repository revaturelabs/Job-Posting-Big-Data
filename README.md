# Job-Posting-Big-Data
Hello! Welcome to the Common Crawl Job Analysis created by Revature Big Data Batch 210104
_______________________________________________________________________________________________________________________________
## Project Description
The purpose of this project is to use the Common Crawl database to analyze job postings in various contexts:  
1) Where do we see relatively fewer tech ads proportional to population?  
2) What companies are posting the most jobs?  
3) What percent of tech job posters post no more than three job ads a month?  
4) Is there a significant spike in tech job postings at the end of business quarters?  
5) Are there general trends in tech job postings over the past year?  
6) What percentage of "Entry Level Jobs" require experience?  
7) What are the top three qualifications or certifications requested by employers?  
8) What percentage of tech job postings require experience in a "Low Code" or "No Code" solution?  

In addition, we pursued additional analysis as time and interested permitted:  
1) Does the distribution of Common Crawl text data provide insight into what jobs are being posted?  
2) What are the most popular programming languages?  
_______________________________________________________________________________________________________________________________
## Technologies Used
* Apache Spark
* Scala
* Scala Metals
* AWS EMR
* AWS S3
* AWS Athena
* VSCode
* SBT
_______________________________________________________________________________________________________________________________
## Getting Started

To download the repo:
```git clone https://github.com/revaturelabs/Job-Posting-Big-Data.git```

For any question, you will likely need to create Amazon Access keys, which are detailed below.  
Other questions may have other requirements. Pay attention to the comments and any additional directions  
listed in the question directories themselves.
_______________________________________________________________________________________________________________________________
## Usage

Navigate to the particular question directory you are interested in running,
(and after following any additional directions in the question itself)
Import the build.sbt using Scala Metals, then run  
```sbt assembly```
then
```spark-submit target/scala.11/<title-of-your-jar-file>```
_______________________________________________________________________________________________________________________________
### Setting Up Amazon Keys
  1) Go to https://console.aws.amazon.com/iam/home?#/security_credentials
  2) Select "Access Keys (access key ID and secret access key)
  3) Create New Access Key (Probably save that text file they tell you to, so you don't lose your secret key)
  4) Add the keys (both of them) to your .bashrc file
   (In Ubuntu)
   - cd .
   - nano .bashrc
   - Add in exports for the keys
  5) Save & exit
_______________________________________________________________________________________________________________________________
### Installing S3CMD 

  1) The first thing you will want to do is install pip. Pip is an installer built with Python to install python libraries. Very similar to apt. To install, go ahead and run this command:sudo apt update the run: sudo apt install python3-pip

  2) Make sure to install python as well if you do not already have it, you can do so with this command sudo apt install python

  3) You will also want to download the s3cmd zip file from here: https://s3tools.org/s3cmd (just download the latest version from source forge)

  4) Unzip the file in your wsl (I used unzip for this, you can download the command using sudo apt install unzip)

  5) Go into the newly created file and run sudo pip3 install s3cmd to get all of the dependencies, then run sudo python3 setup.py install

  6) You will need the python-dateutil which you can install using sudo apt-get install python-dateutil

  7) You will want to run the configuration for s3cmd, but before that, you will need to get an access key and a secret key. To get them, log into your aws account and go to your iam security credentials, you can access it here when you log in: https://console.aws.amazon.com/iam/home?#/security_credentials

  8) Click on access keys, and create a new access key, make sure to save these keys, you will use them next.

  9) Run ./s3cmd --configure and give the application the access key and secret key where it is prompted. Everything else you should be able to leave blank (although for encryption password, I would put in something you can remember, but different from other passwords that you have).

  10) Go ahead and test the connection at the end, if it works, you should be able to continue using s3cmd without issue, if there are problems that occur during any step, let me know, and I will attempt to solve it (and perhaps add it to this README so that others can see it if they have the same problem).


Trouble Shooting
 - s3cmd not found: If you are getting this, it could be that the permissions are not set on the file. run ls -l to check s3cmd to see if it has execute privileges for the owner, if it does not, run chmod 755 ./s3cmd which will give the owner read, write and execute privileges, and read and execute privileges for everyone else.
_______________________________________________________________________________________________________________________________
### Errors on Spark-Submit
If you spark-submit your jar file, and get an error about content length, you'll need to downgrade your java version.

**ON WINDOWS**
```
wget https://launchpad.net/~openjdk-security/+archive/ubuntu/ppa/+build/19763089/+files/openjdk-8-jre_8u265-b01-0ubuntu2~18.04_amd64.deb
wget https://launchpad.net/~openjdk-security/+archive/ubuntu/ppa/+build/19763089/+files/openjdk-8-jdk_8u265-b01-0ubuntu2~18.04_amd64.deb
wget https://launchpad.net/~openjdk-security/+archive/ubuntu/ppa/+build/19763089/+files/openjdk-8-jdk-headless_8u265-b01-0ubuntu2~18.04_amd64.deb
wget https://launchpad.net/~openjdk-security/+archive/ubuntu/ppa/+build/19763089/+files/openjdk-8-jre-headless_8u265-b01-0ubuntu2~18.04_amd64.deb
sudo dpkg -i openjdk-8-jre-headless_8u265-b01-0ubuntu2~18.04_amd64.deb
sudo dpkg -i openjdk-8-jdk-headless_8u265-b01-0ubuntu2~18.04_amd64.deb
sudo dpkg -i openjdk-8-jre_8u265-b01-0ubuntu2~18.04_amd64.deb
sudo dpkg -i openjdk-8-jdk_8u265-b01-0ubuntu2~18.04_amd64.deb
```
Source Link - https://github.com/delta-io/delta/issues/544

**ON MAC**
```
https://github.com/AdoptOpenJDK/openjdk8-binaries/releases/download/jdk8u265-b01/OpenJDK8U-jdk_x64_mac_hotspot_8u265b01.pkg
https://github.com/AdoptOpenJDK/openjdk8-binaries/releases/download/jdk8u265-b01/OpenJDK8U-jre_x64_mac_hotspot_8u265b01.pkg
```
_______________________________________________________________________________________________________________________________
### Build.sbt Errors
You might have some issues in your build.sbt, changing these library dependencies should help:
```
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.7",
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.7",
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.7"
```

_______________________________________________________________________________________________________________________________
### Links to other relevant information/GitHubs

- https://commoncrawl.org/the-data/get-started/
- https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/
- https://skeptric.com/common-crawl-job-ads/
