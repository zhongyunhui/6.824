#!/bin/bash
filePath="./log"
if [ ! -f "$filePath" ]
then
touch $filePath
echo "文件创建完成"
else
rm -rf $filePath
echo "文件已经存在"
fi
[ -e ./fd1 ] || mkfifo ./fd1
exec 3<>./fd1
rm -rf ./fd1
for((i=1; i <= 100; i++))
do
	echo >&3
done

n=0
order="go test -race"
while(($n<=1000))
do
  n=$((n+1))
  read -u3
  {
      echo "当前迭代次数为$n"
      $order >> $filePath
      echo >&3
  }
done
echo "全部迭代完毕"
