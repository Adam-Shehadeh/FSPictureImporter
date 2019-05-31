//Author: Adam Shehadeh
//Date:   6/13/2018
//Desc:   Imports directory of pictures into SQL data table, storing location and the item the picture is associated with.

open System
let ROOT_DIRECTORY = "\\\\zdeviis\\ITEM_IMAGES\\"
let CONN_STRING = "Data Source=zdevzsql01;Initial Catalog=MDM;Integrated Security=True"
module SQLDataLayer =
    open System.IO
    open System.Data.SqlClient

    let conn = new SqlConnection(CONN_STRING)
    let readDirectory = seq { 
        let mutable counter = 0
        for file in Directory.EnumerateFiles(ROOT_DIRECTORY) do       
            yield file
    }

    let truncateTable () = 
        printfn "Truncating image table..."
        let cmd = new SqlCommand("TRUNCATE TABLE AppData.ImageReferenceDictionary", conn)
        conn.Open()
        cmd.ExecuteNonQuery() |> ignore
        conn.Close()
        |> ignore

    let validate_item (item: string) =
        let cmd = new SqlCommand("SELECT COUNT(*) FROM Item.Item WHERE [Item] = '"+ item + "'", conn)
        conn.Open()
        let result =
            match Convert.ToInt32(cmd.ExecuteScalar()) with
            | 0 -> false
            | 1 -> true
            | _ -> true
        conn.Close()
        result

    let insert_row (row: string, ind: int) = 
        
        let mutable Item_Name = row.Replace(ROOT_DIRECTORY, "")
        Item_Name <- Item_Name.Remove(Item_Name.IndexOf(".")).Replace("_", "*")

        let Item = 
            match validate_item Item_Name with
            | true -> Item_Name
            | false -> ""

        let ImageNm = row.Replace(ROOT_DIRECTORY,"") 
        let FullImagePath = row
        let ImageDesc = ""
        let IsActive = "1"
        let InsertDate = DateTime.Now.ToShortDateString()        
        let LastUpdate = DateTime.Now.ToShortDateString()
        let sql = @"
                     DECLARE @EXISTS bit;
                     SET @EXISTS = CASE WHEN (SELECT COUNT(*) FROM AppData.ImageReferenceDictionary WHERE ImageNm = '" + ImageNm + "') > 0 THEN 1 ELSE 0 END;

                    IF @EXISTS = 0 
                        INSERT INTO [AppData].[ImageReferenceDictionary] VALUES ( '" + Item + "', '" + ImageNm + "', '" + ImageDesc + "', " + IsActive + ", '" + InsertDate + "', '" + LastUpdate + "')"
        let cmd = new SqlCommand(sql, conn)
        conn.Open()
        let rows = cmd.ExecuteNonQuery()

        if (rows > 0) then
            printfn "%i. Inserted row: %s" ind ImageNm
        else
            printfn "%i. %s is already mapped in the database and has been ignored by the importer." ind ImageNm
        conn.Close()
        |> ignore

[<EntryPoint>]
let main argv =

    printfn "--------------------------------------------------------------------------"
    printfn "TITLE:             : Item Master/Pulse Image Directory Sync" 
    printfn "AUTHOR             : Adam Shehadeh"
    printfn "CREATED            : 6/13/2018"
    printfn "ROOT DIRECTORY     : %s" ROOT_DIRECTORY
    printfn "CONNECTION         : %s" CONN_STRING
    printfn "--------------------------------------------------------------------------\n"
    printfn "Imports images from %s into SQL Database and maps images the item matching the image's name. Skips duplicates if mapping already exists. See MDM.AppData.ImageReferenceDictionary table to view mappings. \n" ROOT_DIRECTORY

    printfn "Press any key to run the script."
    let input = System.Console.ReadLine()
    

    let mutable counter = 0
    for row in SQLDataLayer.readDirectory do
        SQLDataLayer.insert_row(row, counter)
        counter <- counter + 1

    printfn "-------------------------------------"
    printfn "Import Complete. %i files were processed." counter
    printfn "-------------------------------------"

    let ln = System.Console.ReadLine()
    0 // return an integer exit code
   
