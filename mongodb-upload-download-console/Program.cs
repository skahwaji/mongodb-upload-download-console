using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using Nito.AsyncEx;
using System;
using System.IO;
using System.Threading.Tasks;

namespace MongoDBGridFS
{
    class Program
    {
        private static MongoClient _mongoClient = null;
        private static IMongoDatabase _database = null;
        private static GridFSBucket _gridFSBucket = null;

        private static string UplodaPath = "./upload";
        private static string DownloadPath = "./download";

        public static void UploadFileFromBytes(string FileName)

        {
            Console.WriteLine("Read the file into a byte source");
            byte[] source = File.ReadAllBytes(UplodaPath + "/" + FileName);

            Console.WriteLine("Upload the byte source to gridFS collection");
            ObjectId id = _gridFSBucket.UploadFromBytes(FileName, source);

            Console.WriteLine("Uploaded file Id: " + id.ToString());
        }

        public static void UploadFileFromStream(string FileName)

        {
            Console.WriteLine("Read the file into a stream");
            Stream stream = File.Open(UplodaPath + "/" + FileName, FileMode.Open);

            Console.WriteLine("Prepare the upload options with more metadata");
            var options = new GridFSUploadOptions()
            {
                Metadata = new BsonDocument()
                {
                    {"author", "Shadi Kahwaji"},
                    {"year", 2021}
                }
            };

            Console.WriteLine("Upload stream to the GridFS");
            var id = _gridFSBucket.UploadFromStream(FileName, stream, options);

            Console.WriteLine("Uploaded file Id: " + id.ToString());
        }

        public static async Task DownloadFileAsBytesByName(string FileName)

        {
            var filter = Builders<GridFSFileInfo<ObjectId>>
                                           .Filter.Eq(x => x.Filename, FileName);

            var searchResult = await _gridFSBucket.FindAsync(filter);
            var fileEntry = searchResult.FirstOrDefault();
            if(fileEntry != null)
            { 
                byte[] content = await _gridFSBucket.DownloadAsBytesAsync(fileEntry.Id);
                File.WriteAllBytes(DownloadPath + "/" + fileEntry.Filename, content);
            }
            else
            {
                Console.WriteLine("Cannot find file: '" + FileName + "'");
            }
        }

        public static async Task DownloadFileToStreamByName(string FileName)

        {
            var filter = Builders<GridFSFileInfo<ObjectId>>
                                             .Filter.Eq(x => x.Filename, FileName);

            var searchResult = await _gridFSBucket.FindAsync(filter);
            var fileEntry = searchResult.FirstOrDefault();
            if (fileEntry != null)
            { 
                var file = DownloadPath + "/" + fileEntry.Filename;
                using Stream fs = new FileStream(file, FileMode.CreateNew, FileAccess.Write);
                await _gridFSBucket.DownloadToStreamAsync(fileEntry.Id, fs);
                fs.Close();
            }
            else
            {
                Console.WriteLine("Cannot find file: '" + FileName + "'");
            }
        }

        public static async Task DownloadFileToStreamById(ObjectId Id)

        {
            var filter = Builders<GridFSFileInfo<ObjectId>>
                                            .Filter.Eq(x => x.Id, Id);

            var searchResult = await _gridFSBucket.FindAsync(filter);
            var fileEntry = searchResult.FirstOrDefault();
            if (fileEntry != null)
            {
                var file = DownloadPath + "/" + fileEntry.Filename;
                using Stream fs = new FileStream(file, FileMode.CreateNew, FileAccess.Write);
                await _gridFSBucket.DownloadToStreamAsync(Id, fs);
                fs.Close();
            }
            else
            {
                Console.WriteLine("Cannot find file with Id: '" + Id.ToString() + "'");
            }
        }

        static void Main(string[] args)
        {
            AsyncContext.Run(() => MainAsync(args));
        }

        static async void MainAsync(string[] args)
        {
            Console.WriteLine("Initialize the MongoDB variables");
            _mongoClient = new MongoClient("mongodb://127.0.0.1:27017");
            _database = _mongoClient.GetDatabase("SMB");
            _gridFSBucket = new GridFSBucket(_database, new GridFSBucketOptions
            {
                BucketName = "BearNose",
                ChunkSizeBytes = 1048576, // 1MB
                WriteConcern = WriteConcern.WMajority,
                ReadPreference = ReadPreference.Secondary
            });

            Console.WriteLine("Call to download a file from GridFS");
            await DownloadFileToStreamById(new ObjectId("61b23f03db04ab651a76ca16"));
            Console.WriteLine("Done...press any key to exit.");
            Console.ReadKey();
        }
    }
}
