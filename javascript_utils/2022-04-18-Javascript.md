---
layout: post
title: JavaScript
comments: true
---

Example: Convert Date to Number

```js
// program to convert date to number
// create date
const d1 = new Date();
console.log(d1);

// converting to number
const result = d1.getTime();
console.log(result);
```

{: .box-note}
**Output**
Mon Nov 09 2020 10:52:32 GMT+0545 (Nepal Time)  
1604898452084

JavaScript Program to Write to Console
In this example, you will learn to write a JavaScript program that will allow you to write to the console.
Example: Using console.log()

// program to write to console

// passing number 
console.log(8);

// passing string
console.log('hello');

// passing variable
const x = 'hello';
console.log(x);

// passing function
function sayName() {
    return 'Hello John';
}
console.log(sayName());

// passing string and a variable
const name = 'John';
console.log('Hello ' + name);

// passing object
let obj = {
    name: 'John',
    age: 28
}
console.log(obj);
Output
8
hello
hello
Hello John
Hello John
{
  age: 28,
  name: "John"
}



JavaScript Program to Remove All Whitespaces From a Text
In this example, you will learn to write a JavaScript program that will remove all whitespaces from a text.
To understand this example, you should have the knowledge of the following JavaScript programming topics:
JavaScript String split()
Javascript Array join()
JavaScript Regex

Example 1: Using split() and join()

// program to trim a string

const string = '      Hello World       ';

const result = string.split(' ').join('');

console.log(result);
Output
HelloWorld
In the above program,
The split(' ') method is used to split the strings into individual array elements. ["", "", "", "", "", "", "Hello", "World", "", "", "", "", "", "", ""]
The join('') method merges the array into a string.

Example 2: Using Regular Expression

// program to trim a string

function trimString(x) {

    const result = x.replace(/\s/g,'');
    return result

}

const result = trimString('    Hello World    ');
console.log(result);
Output
HelloWorld



JavaScript Program to Get the Dimensions of an Image
In this example, you will learn to write a JavaScript program that will get the dimensions of an image.
Example: Get Dimensions of an Image

// program to get the dimensions of an image

const img = new Image();

// get the image
img.src = '//cdn.programiz.com/sites/tutorial2program/files/cover-artwork.png';

// get height and width
img.onload = function() {
  console.log('width ' + this.width)
  console.log('height '+ this.height);
}
Output
width 1040
height 922
In the above program, new Image() constructor is used to create an image object.
The Image() constructor creates a new image element instance.


img.src is then used to add the image using an image URL source.
The img.onload() function is used to access the height and width of the image.

JavaScript Program to Pass a Function as Parameter
In this example, you will learn to write a JavaScript program that will pass a function as a parameter.
To understand this example, you should have the knowledge of the following JavaScript programming topics:
JavaScript Function and Function Expressions

Example: Function as Parameter

// program to pass a function as a parameter

function greet() {
    return 'Hello';
}

// passing function greet() as a parameter
function name(user, func)
{

    // accessing passed function
    const message = func();

    console.log(`${message} ${user}`);
}

name('John', greet);
name('Jack', greet);
name('Sara', greet);
Output
Hello John
Hello Jack
Hello Sara
In the above program, there are two functions: name() and greet().
The name() function takes two parameters.
The greet() function is passed as an argument to the name() function.

JavaScript Program to Implement a Queue
In this example, you will learn to write a JavaScript program that will implement a queue.
To understand this example, you should have the knowledge of the following JavaScript programming topics:
JavaScript Array push()
JavaScript Array shift()
JavaScript Methods and this Keyword

A queue is a data structure that follows First In First Out (FIFO) principle. The element that is added first is accessed at first. This is like being in a queue to get a movie ticket. The first one gets the ticket first.

Example: Implement Queue

// program to implement queue data structure

class Queue {
    constructor() {
        this.items = [];
    }
    
    // add element to the queue
    enqueue(element) {
        return this.items.push(element);
    }
    
    // remove element from the queue
    dequeue() {
        if(this.items.length > 0) {
            return this.items.shift();
        }
    }
    
    // view the last element
    peek() {
        return this.items[this.items.length - 1];
    }
    
    // check if the queue is empty
    isEmpty(){
       return this.items.length == 0;
    }
   
    // the size of the queue
    size(){
        return this.items.length;
    }
 
    // empty the queue
    clear(){
        this.items = [];
    }
}

let queue = new Queue();
queue.enqueue(1);
queue.enqueue(2);
queue.enqueue(4);
queue.enqueue(8);
console.log(queue.items);

queue.dequeue();
console.log(queue.items);

console.log(queue.peek());

console.log(queue.isEmpty());

console.log(queue.size());

queue.clear();
console.log(queue.items);
Output
[1, 2, 4, 8]
[2, 4, 8]
8
false
3
[]



In the above program, the Queue class is created to implement the queue data structure. The class includes methods like enqueue(), dequeue(), peek(), isEmpty(), size(), and clear().
A Queue object is created using a new operator and various methods are accessed through the object.
Initially, this.items is an empty array.
The push() method adds an element to this.items.
The shift() method removes the first element from this.items.
The length property gives the length of this.items.


JavaScript Program to Implement a Stack
In this example, you will learn to write a JavaScript program that will implement a stack.
To understand this example, you should have the knowledge of the following JavaScript programming topics:
JavaScript Array push()
JavaScript Array pop()
JavaScript Methods and this Keyword

The stack is a data structure that follows Last In First Out (LIFO) principle. The element that is added at last is accessed at first. This is like stacking your books on top of each other. The book that you put at last comes first.

Example: Implement Stack

// program to implement stack data structure
class Stack {
    constructor() {
        this.items = [];
    }
    
    // add element to the stack
    add(element) {
        return this.items.push(element);
    }
    
    // remove element from the stack
    remove() {
        if(this.items.length > 0) {
            return this.items.pop();
        }
    }
    
    // view the last element
    peek() {
        return this.items[this.items.length - 1];
    }
    
    // check if the stack is empty
    isEmpty(){
       return this.items.length == 0;
    }
   
    // the size of the stack
    size(){
        return this.items.length;
    }
 
    // empty the stack
    clear(){
        this.items = [];
    }
}

let stack = new Stack();
stack.add(1);
stack.add(2);
stack.add(4);
stack.add(8);
console.log(stack.items);

stack.remove();
console.log(stack.items);

console.log(stack.peek());

console.log(stack.isEmpty());

console.log(stack.size());

stack.clear();
console.log(stack.items);
Output
[1, 2, 4, 8]
[1, 2, 4]
4
false
3
[]


In the above program, the Stack class is created to implement the stack data structure. The class methods like add(), remove(), peek(), isEmpty(), size(), clear() are implemented.
An object stack is created using a new operator and various methods are accessed through the object.
Here, initially this.items is an empty array.
The push() method adds an element to this.items.
The pop() method removes the last element from this.items.
The length property gives the length of this.items.


JavaScript Program to Perform Function Overloading
In this example, you will learn to write a JavaScript program that will perform function overloading.
To understand this example, you should have the knowledge of the following JavaScript programming topics:
JavaScript if...else Statement
JavaScript Switch Statement
JavaScript Function and Function Expressions

In programming, function overloading refers to the concept where multiple functions with the same names can have different implementations. However, in JavaScript, if there are multiple functions with the same name, the function that is defined at the last gets executed.
The function overloading feature can be implemented in some other ways.

Example 1: Using if/else-if Statement

// program to perform function overloading

function sum() {  
    
    // if no argument        
    if (arguments.length == 0) {  
        console.log('You have not passed any argument');  
    }

    // if only one argument 
    else if (arguments.length == 1) {  
        console.log('Pass at least two arguments');  
    }

    // multiple arguments
    else {
        let result = 0;
        let length = arguments.length;
    
        for (i = 0; i < length; i++) {  
            result = result + arguments[i];  
        }  
        console.log(result); 
    }  
}

sum();
sum(5); 
sum(5, 9);    
sum(1, 2, 3, 4, 5, 6, 7, 8, 9); 
Output
You have not passed any argument
Pass at least two arguments
14
45



In the above program, the overloading feature is accomplished by using the if/else...if statement.
In JavaScript, the arguments object is automatically available inside a function that represents the passed arguments to a function.
The multiple conditions are addressed to perform actions based on that particular condition.

Example 2: Using switch Statement

// program to perform function overloading
function sum() {
    switch (arguments.length) {
    case 0:
        console.log('You have not passed any argument');
        break;
    case 1:
        console.log('Pass at least two arguments');
        break;
    default:
        let result = 0;
        let length = arguments.length;
    
        for (i = 0; i < length; i++) {  
            result = result + arguments[i];  
        }  
        console.log(result);
        break;
    }
}

sum();
sum(5); 
sum(5, 9);    
sum(1, 2, 3, 4, 5, 6, 7, 8, 9);
Output
You have not passed any argument
Pass at least two arguments
14
45
In the above program, the switch statement is used to accomplish the function overloading functionality. Different conditions result in different actions to be performed.
Share on:




JavaScript Program to Generate a Range of Numbers and Characters
In this example, you will learn to write a JavaScript program that will generate a range of numbers and characters by passing the upper and lower bounds.
To understand this example, you should have the knowledge of the following JavaScript programming topics:
JavaScript String fromCharCode()
JavaScript String charCodeAt()
JavaScript Array map()
JavaScript Generators

Example: Generate Range of Characters

// program to generate range of numbers and characters
function* iterate(a, b) {
  for (let i = a; i <= b; i += 1) {
    yield i
  }
}

function range(a, b) {
    if(typeof a === 'string') {
        let result = [...iterate(a.charCodeAt(), b.charCodeAt())].map(n => String.fromCharCode(n));
        console.log(result);
    }
    else {
        let result = [...iterate(a, b)];
        console.log(result);
    }
}

range(1, 5);
range('A', 'G');
Output
[1, 2, 3, 4, 5]
["A", "B", "C", "D", "E", "F", "G"]
In the above program, a range of numbers and characters is generated between the upper and the lower bounds.
The iterate generator function is used to iterate through lower and upper bounds.
The spread syntax ... is then used to include all the elements returned by the iterate function.
The charCodeAt() method takes in an index value and returns an integer representing its UTF-16 (16-bit Unicode Transformation Format) code.
The map() method iterates through all the array elements.
The fromCharCode() method converts Unicode values into characters.
