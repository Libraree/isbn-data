# isbn-data

Code to download the latest [Open Library editions dump](https://openlibrary.org/developers/dumps), place work identifier and ISBN data within a Sqlite database, then produce a CSV file for upload into Azure Table Storage. This data allows Libraree to find ISBNs related to the book for which you're looking, therefore allowing you to search more broadly within your library services.

## Deliberate Limitations & Assumptions

Libraree assumes that you're looking for English-language books. As such, it will only track ISBN-13 codes beginning 9780 and 9781.

Additionally, if there is only one ISBN-13 for a book, it is excluded from the CSV file and hence from Table Storage. There is no value in holding the data since the API always returns the ISBN code you requested within the array of ISBNs it returns, regardless of whether it's found in Table Storage.

ISBN-10 codes in the Open Library dataset have been upgraded to ISBN-13.

Up to 20 related ISBNs are stored against each ISBN, even if more exist within the dataset. These are chosen purely on ISBN numerical order. This is done to minimise the number of requests on library service catalogues.
