using System;
using System.IO;
using Microsoft.Data.Sqlite;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics;

namespace ean_db_parser {

    class EanParser {
        SqliteConnection db;
        // Хэштаблицы с ID для контроля повторов
        HashSet<Int32> cats = new HashSet<Int32>();
        HashSet<Int32> brands = new HashSet<Int32>();
        HashSet<Int64> eans = new HashSet<Int64>();
        // Счетчик записей
        Int32 row_count = 0;

        void ExecuteNonQuery(String query) {
            var cmd = new SqliteCommand(query, db);
            cmd.ExecuteNonQuery();
        }

        public void CreateDB(String filename) {
            Console.WriteLine($"Создание БД {filename}");
            if (File.Exists(filename)) File.Delete(filename);

            db = new SqliteConnection($"Data Source='{filename}'");
            db.Open();

            ExecuteNonQuery("PRAGMA journal_mode = OFF");
            ExecuteNonQuery("PRAGMA foreign_keys = ON");
            ExecuteNonQuery("PRAGMA read_uncommitted = ON");
            ExecuteNonQuery("PRAGMA synhronous = OFF");
            ExecuteNonQuery("PRAGMA cache_size = -32678");

            ExecuteNonQuery("create table Category(cat_id int primary key, category text)");
            ExecuteNonQuery("create table Brand (brand_id int primary key, brand text)");
            ExecuteNonQuery("create table EAN (ean int primary key, tovar text, cat_id int references Category, brand_id int references Brand)");
            ExecuteNonQuery("BEGIN TRANSACTION");
        }

        public void ParseCSV(String filename) {
            Console.WriteLine($"Загрузка из файла {filename}");
            using (StreamReader sr = new StreamReader(filename, System.Text.Encoding.Default)) {
                // Заголовок
                 String line = sr.ReadLine();
                if(line == null) {
                    Console.WriteLine($"ОШИБКА: Файл пустой {filename}");
                    return;
                }
                Int32 ean_pos = -1, name_pos = -1, cat_id_pos = -1, cat_pos = -1, brand_id_pos = -1, brand_pos = -1;
                var hdr = line.Split('\t');
                for(Int32 i = 0; i < hdr.Length; i++) {
                    if(hdr[i] == "UPCEAN") ean_pos = i;
                    else if(hdr[i] == "Name") name_pos = i;
                    else if(hdr[i] == "CategoryID") cat_id_pos = i;
                    else if(hdr[i] == "CategoryName") cat_pos = i;
                    else if(hdr[i] == "BrandID") brand_id_pos = i;
                    else if(hdr[i] == "BrandName") brand_pos = i;
                }
                Int32[] arr = { ean_pos, name_pos, cat_id_pos, cat_pos, brand_id_pos, brand_pos};
                if(arr.Min() == -1) {
                    Console.WriteLine($"ОШИБКА: Неправильный заголовок в файле {filename}");
                    return;
                }
                Int32 max = arr.Max();
                var ins_ean = new SqliteCommand("insert into EAN (ean, tovar, cat_id, brand_id) values (@ean, @tovar, @cat_id, @brand_id)", db);
                ins_ean.Prepare();
                var ins_br = new SqliteCommand("insert into Brand (brand_id, brand) values (@brand_id, @brand)", db);
                ins_br.Prepare();
                var ins_cat = new SqliteCommand("insert into Category (cat_id, category) values (@cat_id, @category)", db);
                ins_cat.Prepare();
                // Данные
                while ((line = sr.ReadLine()) != null) {
                    var row = line.Split('\t');
                    if(row.Length <= max) {
                        Console.WriteLine($"ОШИБКА: Неверный формат строки '{line}'");
                        continue;
                    }
                    // Категория
                    Int32 cat_id;
                    if(!Int32.TryParse(row[cat_id_pos], out cat_id)) {
                        Console.WriteLine($"ОШИБКА: CategoryID = '{row[cat_id_pos]}'");
                        continue;
                    }
                    if(!cats.Contains(cat_id)) {
                        cats.Add(cat_id);
                        ins_cat.Parameters.Clear();
                        ins_cat.Parameters.AddWithValue("@cat_id", cat_id);
                        ins_cat.Parameters.AddWithValue("@category", row[cat_pos]);
                        ins_cat.ExecuteNonQuery();
                    }
                    
                    // Брэнд
                    Int32 brand_id;
                    if(!Int32.TryParse(row[brand_id_pos], out brand_id)) {
                        Console.WriteLine($"ОШИБКА: BrandID = '{row[brand_id_pos]}'");
                        continue;
                    }
                    if(!brands.Contains(brand_id)) {
                        brands.Add(brand_id);
                        ins_br.Parameters.Clear();
                        ins_br.Parameters.AddWithValue("@brand_id", brand_id);
                        ins_br.Parameters.AddWithValue("@brand", row[brand_pos]);
                        ins_br.ExecuteNonQuery();
                    }
                    
                    // Штрихкод
                    Int64 ean;
                    if(!Int64.TryParse(row[ean_pos], out ean)) {
                        Console.WriteLine($"ОШИБКА: EAN = '{row[ean_pos]}'");
                        continue;
                    } 
                    if(eans.Contains(ean)) {
                        Console.WriteLine($"ОШИБКА: повтор EAN = '{row[ean_pos]}'");
                        continue;
                    } else {
                        eans.Add(ean);
                        ins_ean.Parameters.Clear();
                        ins_ean.Parameters.AddWithValue("@ean", ean);
                        ins_ean.Parameters.AddWithValue("@tovar", row[name_pos]);
                        ins_ean.Parameters.AddWithValue("@cat_id", cat_id);
                        ins_ean.Parameters.AddWithValue("@brand_id", brand_id);
                        ins_ean.ExecuteNonQuery();
                        row_count++;
                        if(row_count % 500000 == 0) {
                            ExecuteNonQuery("COMMIT");
                            ExecuteNonQuery("BEGIN TRANSACTION");
                        }
                    }
                }
                Console.WriteLine($"Загружено {row_count} строк");
            }
        }

        public void CreateIndexes() {
            ExecuteNonQuery("COMMIT");
            Console.WriteLine("Индексирование ...");
            ExecuteNonQuery("create index category_category_idx ON category(category)");
            ExecuteNonQuery("create index brand_brand_idx ON brand(brand)");
            ExecuteNonQuery("create index ean_brand_idx ON ean(brand_id)");
            ExecuteNonQuery("create index ean_cat_idx ON ean(cat_id)");
            ExecuteNonQuery("create index ean_tovar_idx ON ean(tovar)");
        }
    }



    class Program
    {

        static void Main(string[] args)
        {
            if(args.Length == 0) {
                Console.WriteLine("\nСоздание БД SQLite на основе списков EAN-кодов https://github.com/papyrussolution/UhttBarcodeReference/releases \n\n ean_db_parser.exe <folder>");
                return;
            }
            Stopwatch sw = Stopwatch.StartNew();
            var path = args[0];
            var ep = new EanParser();
            ep.CreateDB($"{path}\\ean.db");
            foreach(var f in Directory.GetFiles(path, "*.csv")) ep.ParseCSV(f);
            Console.WriteLine($"Время: {sw.ElapsedMilliseconds / 1000}.{(sw.ElapsedMilliseconds % 1000) / 100:D1} сек.");
            ep.CreateIndexes();
            Console.WriteLine($"Время: {sw.ElapsedMilliseconds / 1000}.{(sw.ElapsedMilliseconds % 1000) / 100:D1} сек.");
        }
    }
}
