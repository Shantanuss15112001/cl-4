from mrjob.job import MRJob
import csv

class TitanicAnalysis(MRJob):

    def mapper(self, _, line):
        row = next(csv.reader([line]))
        # Check if the row contains valid data
        if len(row) == 12 and row[0] != 'PassengerId':
            survived = int(row[1])
            pclass = int(row[2])
            age = float(row[5]) if row[5] else None
            gender = row[4]
            
            # Emit key-value pairs for average age of people who died (survived=0) by gender
            if survived == 0:
                yield (gender, (age, 1))
            
            # Emit key-value pairs for number of survivors per class
            if survived == 1:
                yield (pclass, 1)

    def reducer(self, key, values):
        if isinstance(key, str):  # Calculating average age for gender
            total_age = 0
            total_count = 0
            for age, count in values:
                if age is not None:
                    total_age += age * count
                    total_count += count
            if total_count > 0:
                yield (key, total_age / total_count)
        else:  # Counting survivors per class
            total_count = sum(values)
            yield (f"Survivors in Class {key}", total_count)

if __name__ == '__main__':
    TitanicAnalysis.run()
