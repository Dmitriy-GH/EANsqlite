Создание БД SQLite на основе списков EAN-кодов 
https://github.com/papyrussolution/UhttBarcodeReference/releases

Структура БД:

Категории
Category(cat_id int primary key, category text)

Брэнды
Brand (brand_id int primary key, brand text)

Штрихкоды
EAN (ean int primary key, tovar text, cat_id int references Category, brand_id int references Brand)


Для создания скачать uhtt_barcode_ref_all.7z, распаковать CSV в папку, затем запускать

run.cmd <папка>

PS Предварительно установить .Net core https://dotnet.microsoft.com/download/dotnet-core/current/runtime
