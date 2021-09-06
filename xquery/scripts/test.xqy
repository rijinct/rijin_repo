let $books := (doc("/Users/rthomas/Desktop/Rijin/rijin_git/xquery/conf/books.xml")/books/book)
return <results>
    {
        for $x in $books
        where $x/price>30
        order by $x/price
        return $x/title
    }
</results>
