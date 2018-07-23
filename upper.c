#include <stdio.h>

char* str_to_upper(char *str)
{
  char *ps = str;

  while (*ps)
  {
    *ps = toupper(*ps);
    ps++;
  }
  return(str);

} 

