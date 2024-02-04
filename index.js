import got from 'got';
import { createGunzip } from 'zlib';
import { parse } from 'csv-parse';
import { finished } from 'stream/promises';
import fs from 'fs/promises';
import fsSync from 'fs';
import { AsyncDatabase } from 'promised-sqlite3';

const lastUpdated = '0001-01-01T00:00:00.000000';

const fileExists = path => fs.stat(path).then(() => true, () => false);

function convISBN10toISBN13(str) {
    let c;
    let checkDigit = 0;
    let result = '';

    c = '9';
    result += c;
    checkDigit += (c - 0) * 1;

    c = '7';
    result += c;
    checkDigit += (c - 0) * 3;

    c = '8';
    result += c;
    checkDigit += (c - 0) * 1;

    for (let i = 0; i < 9; i++ ) {  // >
        c = str.charAt(i);
        if ( i % 2 == 0 )
            checkDigit += (c - 0) * 3;
        else
            checkDigit += (c - 0) * 1;
        result += c;
    }
    checkDigit = (10 - (checkDigit % 10)) % 10;
    result += (checkDigit + '');

    return ( result );
}

if (await fileExists('./openlibrary.db'))
    await fs.unlink('./openlibrary.db');

const db = await AsyncDatabase.open('./openlibrary.db');
await db.run('CREATE TABLE LookupTemp ( WorkId text, Isbn text );');
await db.run('CREATE TABLE Lookup ( WorkId text, Isbn text );');

let index = 1;

console.log('Starting ..');
const started = new Date();

const parser = got.stream('https://openlibrary.org/data/ol_dump_editions_latest.txt.gz', { followRedirect: true })
    .pipe(createGunzip())
    .pipe(parse({
        delimiter: '\t',
        quote: false
    }));

parser.on('readable', async () => {
    let record; 
    
    while ((record = parser.read()) !== null) {
        const obj = JSON.parse(record[4]);

        if (index % 10000 == 0)
            console.log(`Index: ${index} ..`);

        if ((obj.last_modified?.value ?? '9999-01-01') < lastUpdated)
            continue;

        if (!obj.works)
            continue;

        if (!obj.languages || obj.languages.count == 0 || obj.languages.find(x => x.key == '/languages/eng')) {
            const isbns = [];

            if (obj.isbn_10) {
                for (let isbn of obj.isbn_10) {
                    const indiv = isbn.split(',');

                    for (let i of indiv) {
                        isbns.push(convISBN10toISBN13(i.replace(/[^0-9]/g, '')));
                    }
                }
            }

            if (obj.isbn_13) {
                for (let isbn of obj.isbn_13) {
                    const indiv = isbn.split(',');

                    for (let i of indiv) {
                        isbns.push(i.replace(/[^0-9]/g, ''));
                    }
                }
            }

            if (isbns.length == 0)
                continue;

            const works = obj.works.map(x => x.key.replace('/works/', ''));
            const finalIsbns = [...new Set(isbns)];

            for(let work of works) {
                for (let isbn of finalIsbns
                    .filter(x => /978[01][0-9]{9}/.test(x))) {
                    await db.run('INSERT INTO LookupTemp VALUES (?, ?);', [ `${work.trim()}`, `${isbn}` ]);
                }
            }
        }

        index++;
    }
});

await finished(parser);

console.log('Indexing ..');

await db.run('CREATE INDEX WorkId_Index ON LookupTemp(WorkId);');
await db.run('CREATE INDEX Isbn_Index ON LookupTemp(Isbn);');

console.log('Populating Lookups ..');
await db.run('INSERT INTO Lookup (WorkId, Isbn) SELECT DISTINCT WorkId, Isbn FROM LookupTemp ORDER BY WorkId, Isbn');
await db.run('DROP TABLE LookupTemp;')
await db.run('CREATE INDEX WorkId_Index ON Lookup(WorkId);');
await db.run('CREATE INDEX Isbn_Index ON Lookup(Isbn);');

console.log('Shrinking ..');

await db.run('VACUUM;');

console.log('Creating CSV ..');

index = 0;

try {
    index++;
    
    const isbnCsv = fsSync.openSync('./isbns.csv', 'w' );
    fsSync.writeSync(isbnCsv, '"PartitionKey","RowKey","Related"\n');

    const isbns = await db.all('SELECT DISTINCT Isbn FROM Lookup ORDER BY Isbn');

    for (let i of isbns) {
        if (index % 10000 == 0)
            console.log(`CSV: ${index} ..`);

        const rows = await db.all(`SELECT      DISTINCT l2.Isbn
            FROM        Lookup l1
            INNER JOIN  Lookup l2 ON l1.WorkId = l2.WorkId
            WHERE       l1.Isbn = '${i.Isbn}'
            LIMIT 20`);

        if (rows.length < 2)
            continue;

        fsSync.writeSync(isbnCsv,`"${i.Isbn.substring(0, 6)}","${i.Isbn.substring(6)}","${JSON.stringify(rows.map(x => x.Isbn)).replace(/"/g, '""')}"\n`);
    }
    
    fsSync.closeSync(isbnCsv);
}
catch(e) {
    console.error(e);
}

await db.close();

console.log(`Finished! Took ${(new Date() - started) / 1000 / 60 / 60} hours.`);