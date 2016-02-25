if [ "$#" -lt 1 ]; then
	echo "Usage [filesize]"
	exit 125
fi

java -cp target/shared-memory-tera-sort.jar terasort.hawk.iit.edu.TeraSort 2 2 500 100 $1 400
~/GenSort/gensort-linux-1.5/64/valsort dataset_final
