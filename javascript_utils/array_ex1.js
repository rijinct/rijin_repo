/*
Write a JavaScript program which accept a string as input and swap the case of each character. 
For example if you input 'The Quick Brown Fox' the output should be 'tHE qUICK bROWN fOX'.
*/

let val = 'come Here'
let upper_alp = 'QAZWSXEDCRFVTGBYHNUJMIKOLP'
let lower_alp = 'qazwsxedcrfvtgbyhnujmikolp'
let results = []
for (var x = 0; x<val.length; x++)
{
    if (upper_alp.indexOf(val[x]) !== -1)
    {
        results.push(val[x].toLowerCase())
    }
    else if (lower_alp.indexOf(val[x]) !== -1)
    {
        results.push(val[x].toLocaleUpperCase())
    }
    else 
    {
        results.push(val[x])
    }
}

console.log(results.join(''))