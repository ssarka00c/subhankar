#!/bin/sh

if [ `id -nu` != "hdfs" ];then
        echo "This script needs to be executed with user hdfs"
        exit 1
fi

export WORKDIR=/home/hdfs/support/snapshotManager
export dayReatin=3
export LOOKUP=$WORKDIR/snapshot.lookup


i=0
arr=()
arr2=()
while [ $i -lt $dayReatin ]
do

arr[$i]=`date --date="$i day ago" +%d-%m-%Y-%H-%M-%S`
arr2[$i]=`date --date="$i day ago" +%d-%m-%Y`
((i=$i+1))

done

echo "Valid Snapshot Dates ${arr[*]}"

snaps=()

for line in `cat $LOOKUP`
do
unset snaps
echo "Started For $line"
snaps=(`hadoop fs -ls $line/.snapshot/|grep ^d|awk -F'/' '{print $NF}'|tr -s '\n' ' '`)
echo "Already Present Snapshots Are ${snaps[*]}"

snapshotPrefix="`echo $line|awk -F'/' '{print $NF}'`"

cnt=${#arr2[@]}
k=0
arr3=()
while [ $k -lt $cnt ]
do
    arr3[$k]=" -e ${snapshotPrefix}-${arr2[$k]}"
    ((k=$k+1))
done

newSnapshotName="${snapshotPrefix}-${arr[0]}"
echo "Creating New Snapshot for $line :- $newSnapshotName"
hadoop dfs -createSnapshot $line $newSnapshotName
if [ $? -eq 0 ];then
        echo "Snapshot Created Successfully"
else
        echo "Snapshot Creation Failed"
        exit 1
fi

#From the snaps[] array find the iones which are not required and delete them
lengthSnaps="${#snaps[@]}"
j=0
delFlag=0
        while [ $j -lt $lengthSnaps ]
        do
#set -x
        cmd='echo ${snaps[$j]}|grep ${arr3[*]}|wc -l'
        out=`eval $cmd`

                if [ $out -eq 0 ];then
                        echo "Deleting Snapshot ${snaps[$j]}"
                        hadoop dfs -deleteSnapshot $line ${snaps[$j]}
                        if [ $? -ne 0 ];then
                                echo "Snapshot Could Not Be Deleted"
                        fi
                fi


        ((j=$j+1))

        done

done
