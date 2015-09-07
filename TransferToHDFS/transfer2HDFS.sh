#!/bin/bash
######################################################################################
#
#			Author :: Subhankar Dey Sarkar
#			Hadoop Administrator 
#
######################################################################################
#set -x
export user=""
export pass=""
export hdfs_path=""
export local_path=""
export environ="prod"
export overwrite="false"
export permission="750"

export RED='\033[0;31m'
export GREEN='\033[1;32m'
export NC='\033[0m'

function usage()
{
echo -e "\n${GREEN}Usage \\n \
-u Username\n \
-p Password,This is not your CADA password rather provided by Hadoop Admins specially to use this program\n \
-h HDFS PATH, should be till filename in an existing folder in HDFS, the file name of the source and the destination can be same or different\n \
-l Local File Path, Multiple File Are not Allowed, Absolute Path is better, No Folders Allowed\n \
-e (Optional) MELD Enviornment To Push Data Valid Values Are prod, stage or dev (all small) , default prod when not specified\n \
-o (Optional) Overwrite true/false default false\n \
-x (Optional) Permission default 750\n${NC} \

Try only -c switch to know the author and contact"

echo -e "\nExample <path_to_script>/transferToMeld.sh -u ssarka00c -p iWontTell -h /user/ssarka00c/newfilename.txt -l /home/ssarka00c/sourcefile.txt -e dev -o false -x 750"
exit 1
}

if [ $# -eq 0 ];then
usage
exit 1
fi

chk_curl=`which curl 2>/dev/null`
if [ -z "$chk_curl" ];then
echo -e "\n${RED}curl is necessary for the programme to run, please either install it or if installed already export it into PATH${NC}"
exit 1
fi

if [ $# -eq 0 ];then
usage
fi


while getopts u:p:h:l:e:o:x:c opt; do
  case "$opt" in
    u)
      user="$OPTARG"
      ;;

    p)
      pass="$OPTARG"
      ;;

    h)
      hdfs_path="$OPTARG"
      ;;

    l)
      local_path="$OPTARG"
       if [ -f "$local_path" ]; then
          echo "Do Nothing" > /dev/null
      else
          echo -e "\n${RED}The object with -l is either not a file or does not exists${NC}"
          usage
          exit 1
       fi

       if [ -r "$local_path" ]; then
          echo "Do Nothing" > /dev/null
      else
          echo -e "\n${RED}`id -nu` does not have permission to read the file specified with -l${NC}"
          usage
          exit 1
       fi
      ;;

    e)
	environ=""
	environ="$OPTARG"
	if [ "${environ}" = "prod" ] || [ "${environ}" = "stage" ] || [ "${environ}" = "dev" ] || [ "${environ}" = "dr" ];then
		echo "Environment To Push Data To $environ"
	else
		echo  -e "\n${RED}Not A Valid Environment Value with -e option${NC}"
		usage
		exit 1
	fi
	;;

    o)
      overwrite="$OPTARG"
	if [ "$overwrite" = "true" ] || [ "$overwrite" = "false" ]; then
	   echo "Do Nothing" > /dev/null
	else
	   echo -e "\n${RED}Overwite should either be true or false all in small letters${NC}"
	   usage
           exit 1
        fi
      ;;


    x)
      perm="$OPTARG"
      newperm=`echo $perm|grep [0-7][0-7][0-7]`
      if [ "$perm" = "$newperm" ];then
         echo "Do Nothing" > /dev/null
      else
        echo -e "\n${RED}Not A valid permission selected by -x, it should be in the format [0-7][0-7][0-7] , eg 754, default is 750${NC}"
        usage
        exit 1
      fi
     ;;

    c)
     echo -e "${GREEN}Author  :: Subhankar Dey Sarkar${NC}"
     echo -e "${GREEN}Contact :: s.deysarkar@yahoo.com"
     exit 0
     ;;

    *)
      usage
      ;;
    

    ?)
      usage
      exit 1
      ;;
  esac
done

#Check if we have valid values on all variables by now

if [ -z "$user" ];then
echo -e "\n${RED}User Name cannot be empty, please specify with -u option${NC} "
exit 1
fi

if [ -z "$pass" ];then
echo -e "\n${RED}Password Cannot be empty, this is not CADA password for the user, a seperate password provided by Hadoop Admins. Contact them if you dont have one. You wont be able to use the program with you having the password.Provide password with -p option${NC}"
usage
exit 1
fi

if [ -z "$hdfs_path" ];then
echo -e "\n${RED}HDFS PATH on the Cluster where you want to place the file should not be empty, it should end with filename and not folder name. The folder should be an existing folder on HDFS.${NC} "
usage
exit 1
fi


if [ -z "$local_path" ];then
	echo -e "\n${RED}Local Path Cannot be empty, it needs to be a valid file${NC} "
	exit 1
else
	if [ -r "$local_path" ];then
		echo "Local File Exists And Have Read Permissions"
			if [ `ls -l $local_path|wc -l` -ne 1 ];then
				echo "Multiple Files Are Not Allowed"
				exit 1
			fi
	else
		echo -e "\n${RED}File Does Not Exists Or Does Not Have Read Access For `id -nu` specified by -l switch${NC}"
		exit 1
	fi
fi


if [ -z "$environ" ];then

	echo -e "\n${RED}Environment Not Set Properly, Where To Push The File${NC} "
	usage
	exit 1

fi


echo "Trying to Authenticate and Fetch Remote Active Namenode Info"
val=`curl "http://auth-server-url:9080/apis/AuthenticateTransUserVer2.jsp?username=$user&token=$pass&env=$environ" -m 10 2>/dev/null|grep 'RES'`
res=""
status=""
nn=""

res=`echo $val|awk -F"," '{print $1}'`
if [ "$res" = "RES" ];then
	status=`echo $val|awk -F"," '{print $2}'`
	if [ "$status" = "success" ];then
		nn=`echo $val|awk -F"," '{print $3}'`
		if [ -z "$nn" ];then
			echo -e "\n${RED}Name Node Info Not Received Properly, Contact Admins${NC}"
			echo "MSG:$val"
			exit 1
		fi
		
		## Verify string to check auth url is good
		newnn=`echo $nn|grep 'auth-server-url'`
		if [ "$newnn" != "$nn" ];then
			echo -e "\n${RED}Name Node Info Not Received Properly, Contact Admins${NC}"
                        echo "MSG:$val"
                        exit 1
		fi
	else
		echo -e "\n${RED}Authentication Failure${NC}"
		exit 1
	fi
else
	echo -e "\n${RED}Authentication or Remote NN Failure, Contact Hadoop Admin Group${NC}"
	echo "Msg:$val"
fi

echo "Authenticated Successfully"
echo $val

echo "Checking if remote hdfs directoty exists"
dir=`dirname $hdfs_path`
msg=`curl http://${nn}:50070/webhdfs/v1${dir}?op=GETFILESTATUS 2>/dev/null`
len=`echo $msg|grep 'type'|grep 'DIRECTORY'|wc -l`

if [ $len -ne 1 ];then
	echo -e "\n${RED}The HDFS Path provided does not contain a valid directory${NC}\n"
	echo $msg
	exit 1
else
	echo "HDFS Provided is a valid Directory"
	echo $msg
fi

echo "Preparing File Transfer"

#check if directory exists or else create
tdir=""/tmp/.transferToMeld""
if [ -d "$tdir" ];then
	echo "Temporary Directory present, good to Proceed"
else
	echo "Temporary Directory not present will create it"
	mkdir $tdir
	if [ $? -ne 0 ];then
		echo -e "\n${RED}Unable to Create Temporary Directory, check permission on /tmp folder for user `id -nu`${NC}\n"
		exit 1
	else
		echo "Directory Created... $tdir"
		chmod 777 $tdir
	fi
fi

tf="$tdir/pidfile.$$"

####### NEW ADD VER 2 , Check if -h is a directory
echo "Checking If HDFS location Is Not A Directory, It Needs To Be A File"

msgnew=`curl http://${nn}:50070/webhdfs/v1${hdfs_path}?op=GETFILESTATUS 2>/dev/null`
lennew=`echo $msg|grep 'type'|grep 'DIRECTORY'|wc -l`

if [ $lennew -ne 0 ];then
        echo "HDFS Path Specified by -h is not a existing Directory"
else
        echo -e "\n${RED}HDFS Path Provided With -h switch Needs to Be A File Path In Valid Directory , Cannot be A Directory Itself${NC}\n"
        echo $msgnew
	exit 1
fi


###### END NEW VER 2
#set -x
curl -v -X PUT "http://${nn}:50070/webhdfs/v1${hdfs_path}?op=CREATE&user.name=${user}&overwrite=${overwrite}&permission=${permission}" 2> $tf

chk_hdr_cnt=`cat $tf|grep 'HTTP/1.1 307 TEMPORARY_REDIRECT'|wc -l`
if [ "$chk_hdr_cnt" -ne 1 ];then
	echo -e "\n${RED}File Preparation Failed${NC}\n"
	echo "MSG:"
	cat $tf
	rm -f $tf
	exit 1
fi

loc_cnt=`cat $tf|grep 'Location:'|wc -l`
if [ "$loc_cnt" -ne 1 ];then
        echo -e "\n${RED}File Upload Location Determined is not ok${NC}\n"
        echo "MSG:"
        cat $tf
        rm -f $tf
        exit 1
fi

upload_url=`cat $tf|grep 'Location:'|awk -F'http://' '{print "http://"$NF}'|sed 's/
//g'`

tfc=`cat $tf|wc -l`

echo "Uploading File to HDFS... Starting `date`"
#set -x
curl -v -X PUT -T "${local_path}" ${upload_url} &> "$tf.1"
if [ $? -ne 0 ];then
	echo "MSG To Be Sent To Admins is Below"
	echo "---------------------------------"
	cat $tf
	cat "$tf.1"
	echo -e "\n${RED}Failed To Upload File${NC}"
	rm -f "$tf" "$tf.1"
	exit 1
else
	echo "End : `date`"
	crt_cnt=`cat "$tf.1"|grep 'HTTP/1.1'|grep Created|wc -l`
	if [ $crt_cnt -ge 1 ];then
		echo -e "\n${GREEN}File Uploaded Successfilly.... Checking Consistency${NC}"
		file_chk_msg=`curl http://${nn}:50070/webhdfs/v1${hdfs_path}?op=GETFILESTATUS 2>/dev/null`
		file_size=`echo $file_chk_msg|grep accessTime|awk -F'"length":' '{print $2}'|awk -F"," '{print $1}'|sed '/^$/d'`
		lf_size=`ls -l $local_path|awk '{print $5}'`
		
		echo -e "HDFS File Size : $file_size \nLocal File Size : $lf_size"
		if [ "$file_size" != "$lf_size" ];then
			echo "MSG"
			echo $file_chk_msg
			rm -f "$tf" "$tf.1"
			echo -e "\n${RED}Consistency Check Failed, Please verify Manually on HDFS${NC} "
			exit 1
		else
			# All Success Criteria in This Block
			echo -e "\nFile Consistency Verified in terms of size"
			echo -e "\n${GREEN}SUCCESS${NC}"
			rm -f "$tf" "$tf.1"
			exit 0
		fi
	else
		#Determine is File Already exusts
		if [ `cat $tf.1|grep 'FileAlreadyExistsException'|wc -l` -ge 1 ];then
			echo "MSG To Be Sent To Admins is Below"
	                echo "---------------------------------"
        	        cat $tf
                	cat "$tf.1"
			echo -e "\n${RED}File Already Exists in HDFS, to overwrite set -o true, default is false${NC}"
                	rm -f "$tf" "$tf.1"
                	exit 1
		fi

                echo "MSG To Be Sent To Admins is Below"
	        echo "---------------------------------"
        	cat $tf
        	cat "$tf.1"
        	rm -f "$tf" "$tf.1"
		echo -e "\n${RED}Either the file already exists, to overwrite use -o true OR Not Able To determine if the file was uploaded properly${NC} "
                exit 1
 
	fi				
 
fi



