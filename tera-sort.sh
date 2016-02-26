if [ "$#" -lt 5 ]; then
	echo "Usage [filesize]"
	exit 125
fi

java -cp target/shared-memory-tera-sort.jar terasort.hawk.iit.edu.TeraSort $1 $2 $3 $4 $5 $6
~/GenSort/gensort-linux-1.5/64/valsort dataset_final
