if [ "$#" -lt 5 ]; then
	echo "Usage [filesize]"
	exit 125
fi


java -Xmx2048m -cp shared-memory-tera-sort.jar terasort.hawk.iit.edu.TeraSort $1 $2 $3 $4 $5 $6
printf "\nvalsort result:\n"
~/GenSort/64/valsort $6'dataset_final'
