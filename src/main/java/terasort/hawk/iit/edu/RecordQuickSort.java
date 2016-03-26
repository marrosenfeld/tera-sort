package terasort.hawk.iit.edu;

/**
 * @author mrosenfeld Implementation of quick sort for records
 */
public class RecordQuickSort {
	private Record recordArray[];
	private int arrayLength;

	public Record[] sort(Record[] records) {

		if (records == null || records.length == 0) {
			return records;
		}

		this.recordArray = records;
		arrayLength = records.length;
		recordQuickSort(0, arrayLength - 1);
		return recordArray;
	}

	private void recordQuickSort(int lowerIndex, int higherIndex) {

		int i = lowerIndex;
		int j = higherIndex;
		// calculate pivot record
		Record pivot = recordArray[lowerIndex + (higherIndex - lowerIndex) / 2];

		// Divide into two arrays
		while (i <= j) {

			while (recordArray[i].compareTo(pivot) < 0) {
				i++;
			}
			while (recordArray[j].compareTo(pivot) > 0) {
				j--;
			}
			if (i <= j) {
				exchangeRecords(i, j);
				// move index to next position on both sides
				i++;
				j--;
			}
		}
		// call quickSort() method recursively
		if (lowerIndex < j)
			recordQuickSort(lowerIndex, j);
		if (i < higherIndex)
			recordQuickSort(i, higherIndex);
	}

	private void exchangeRecords(int i, int j) {
		Record temp = recordArray[i];
		recordArray[i] = recordArray[j];
		recordArray[j] = temp;
	}

}
