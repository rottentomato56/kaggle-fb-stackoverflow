def mapper(line):
  post_sections = line.strip().split('\t')
  
  post_id = post_sections[0]
  if len(post_sections) < 4:
    print line
      

