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
        private static string FilePathToUpload = "./upload/FacadePattern.pdf";
        private static string DownloadPath = "./download";
        private static string FileName = "FacadePattern.pdf";

        public static void UploadFile()

        {
            Console.WriteLine("Read the file into a byte source");
            byte[] source = File.ReadAllBytes(FilePathToUpload);

            Console.WriteLine("Upload the byte source to gridFS collection");
            ObjectId id = _gridFSBucket.UploadFromBytes(FileName, source);

            Console.WriteLine("Uploaded file Id: " + id.ToString());
        }

        public static void UploadFileFromStream()

        {
            Console.WriteLine("Read the file into a stream");
            Stream stream = File.Open(FilePathToUpload, FileMode.Open);

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

        public static async Task DownloadFile()

        {
            var filter = Builders<GridFSFileInfo<ObjectId>>
                                           .Filter.Eq(x => x.Filename, FileName);

            var searchResult = await _gridFSBucket.FindAsync(filter);
            var fileEntry = searchResult.FirstOrDefault();
            byte[] content = await _gridFSBucket.DownloadAsBytesAsync(fileEntry.Id);

            File.WriteAllBytes(DownloadPath + "/sample2.pdf", content);

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

            //Console.WriteLine("Call to upload the file from upload folder");
            //UploadFileFromStream();
            
            Console.WriteLine("Call to download a file from GridFS");
            await DownloadFile();
            Console.ReadKey();
        }
    }
}
