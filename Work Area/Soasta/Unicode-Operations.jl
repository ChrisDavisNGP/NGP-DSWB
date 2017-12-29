## Unicode operators and Symbols

#Julia is a language typically used for scientific data analysis, and as such, it supports symbols that mathematicians and statisticians are accustomed to.  In this tutorial,
#we'll learn how to enter these operators with a traditional keyboard.

### Unicode Set Operators

#Julia supports several Math operators with unicode codepoints as the following examples will show:

# Using \in<Tab> gives you the ∈ operator to check if an element is in a Set
10 ∈ [10, 20, 30]

# And conversely \ni<Tab> to get the is member of ∋ operator
[10, 20, 30] ∋ 10

# And the negation of the above is \notin<Tab>: ∉
10 ∉ [10, 20, 30]

# Using \subseteq<Tab> gives you ⊆ to check if one set is a subset of another
[1, 2, 5] ⊆ [1, 2, 3, 4, 5]

[1, 2, 5] ⊆ [1, 2, 3, 8, 9, 10]

# Use \cup<Tab> for ∪ to get the Union of two sets
[1, 2, 3] ∪ [3, 5, 7]

# And \cap<Tab> to get the intersection: ∩
[1, 2, 3] ∩ [3, 5, 7]

#You can read more about Iterations and Set operations in the [documentation for Collections](http://julia.readthedocs.org/en/latest/stdlib/collections/)

### Unicode variable names

#You can also use unicode characters in variable names.  Either as the entire variable, or as part of a variable.  For example, `\alpha<Tab>` for `α` or `s\^2<Tab>` for `s²`

α = 20

s² = 35

α + s²

#Note that `²` is not an operator, so you cannot use it to get the square of a variable

x = 20
x²

#Instead, use the `^` operator to raise the operand on the left to the operand on the right:

α ^ 2

### Matrix operators

#You can use `\cdot` for a dot product and `\times` for a cross product between matrices

# This should give us 1*5 + 2*10 + 3*15
[1, 2, 3] ⋅ [5, 10, 15]

# This should give us a new matrix with values 2*6-3*5 = -3, 3*4-1*6 = 6, 1*5-2*4 = -3
[1, 2, 3] × [4, 5, 6]

#The Khan Academy has a [video about the cross product](https://www.khanacademy.org/math/linear-algebra/vectors_and_spaces/dot_cross_products/v/linear-algebra-cross-product-introduction) if you're not familiar with it.

#Remember that in Julia vectors, the comma operator separates rows.  You could also use a semicolon.  To separate elements within a row, use `<Space>`.

[1, 2, 3]

[1 2 3]

#If you have multiple elements on a row, then you *MUST* use a semi colon to separate rows.

[1 2 3; 4 5 6]

### LaTeX symbol reference

#You can get a [list of LaTeX Math Symbols](http://oeis.org/wiki/List_of_LaTeX_mathematical_symbols)

#Use Google to find anything that isn't listed there.  For example, search for "[cross product in LaTeX](https://www.google.com/search?q=cross+product+in+latex)"
