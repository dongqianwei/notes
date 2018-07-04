## tips when learning vbs

### Chapter2

1.redim array size and preserve original content

```VB
Dim temp()
ReDim Preserve temp(100)
```

2. user define type:

```VB
Type Employee
Name as String
Salary as Currency
Years as Integer
End Type
```

3. constants
```VB
Const Path_Name = "C://temp"
```

# Chapter3

1. Condition
```VB
If <Condition> Then
  xxx
Else
  xxx
End If

'If in one line'
If <Condition> Then xxx
```

2. '=' means equals

3. And, Or

4. Select/Case
```VB
Sub Test_Case (Grade)
  Select Case Grade
    Case 1
      Msgbox "Grade 1"
    Case 2, 3
      Msgbox "Grade 2 or 3"
    Case 4 To 6
      Msgbox "Grade 4, 5 or 6"
    Case Is > 8
      MsgBox "Grade is above 8"
    Case Else
      Msgbox "Grade not in conditional statements"
  End Select
End Sub
```

5. Loops

5.1 For..Next.. Loops
```VB
For n = 3 to 12 Step 3
  MsgBox n
Next n
```

5.2 For Each Loops
```VB
Dim oWSheet As Worksheet
For Each oWSheet In Worksheets
  MsgBox oWSheet.Name
Next oWSheet
```

5.3 Do Until Loops
```VB
x = 0
Do Until x = 100
  x = x + 1
Loop
```

5.4 While..Wend Loops
```VB
x = 0
While x < 50
  x = x + 1
Wend
```
5.5 Exit Loops

Exit For / Exit Do

# Chapter 4
## String

1. Concatenation

&

2. substring

Mid/Left/Right

3. string to number

Val

4. upper, lower

UCase/LCase

5. in string

Instr

6. length 

Len

7. value to string

CStr

8. value to int

CInt


