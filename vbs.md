## tips when learning vbs

### Chapter2

* redim array size and preserve original content

  ```VBS
  Dim temp()
  ReDim Preserve temp(100)
  ```

* user define type:

  ```VBS
  Type Employee
  Name as String
  Salary as Currency
  Years as Integer
  End Type
  ```

* constants
  ```VBS
  Const Path_Name = "C://temp"
  ```

### Chapter3

* Condition

  ```VBS
  If <Condition> Then
    xxx
  Else
    xxx
  End If

  'If in one line'
  If <Condition> Then xxx
```

* '=' means equals

* And, Or

* Select/Case
  ```VBS
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

* Loops

** For..Next.. Loops
  ```VBS
  For n = 3 to 12 Step 3
    MsgBox n
  Next n
  ```
** For Each Loops
  ```VBS
  Dim oWSheet As Worksheet
  For Each oWSheet In Worksheets
    MsgBox oWSheet.Name
  Next oWSheet
  ```

** Do Until Loops
  ```VBS
  x = 0
  Do Until x = 100
    x = x + 1
  Loop
  ```

** While..Wend Loops
  ```VBS
  x = 0
  While x < 50
    x = x + 1
  Wend
  ```

** Exit Loops

  Exit For / Exit Do

### Chapter 4
#### String

* Concatenation

  &

* substring

  Mid/Left/Right

* string to number

  Val

* upper, lower

  UCase/LCase

* in string

  Instr

* length 

  Len

* value to string

  CStr

* value to int

  CInt


### Chapter 7
#### Debugging


* stop Function

* error handlering

  ```VBS
  Sub Test_Error()
  On Error GoTo err_handler
  temp = Dir("d:\*.*")
  Exit Sub
  err_handler:
  If Err.Number = 71 Then
    MsgBox "The D drive is not ready"
  Else
    MsgBox "An error happend"
  End If
  End Sub
  ```

  ```VBS
  Resume
  'just resume to next statement, skip error statement'
  On Error Resume Next
  ```

* Trun Off Error Handling
  ```VBS
  On Error Resume Next
  On Error GoTo 0
  ```

* User raise Error
  ```VBS
  Error 71
  'Or'
  Err.Raise(71)
  'regenerate current Err'
  Error Err
  ```

## Chapter 9
### Dialogs

* UserForm.Show

* UserForm.Hide
