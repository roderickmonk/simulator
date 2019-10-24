# Python: get_values ()
remaining.trade.size.quadrant <- function (th, market, price.depths, depths, i.limit)
{
  
  if( i.limit < 0 ) #min trade size in bitcoins
  {stop("i.limit must be a positive number")}
  
  th.price.depth.output <- th.price.depth(th, market, i.limit)
  
  #use depth vector to get remaining trade size at each depth
  remaining.depth <- lapply(1:length(depths), function(x)
    {
    remaining.size.depth (th.price.depth.output, depths[x])
    }
    )
    
  #use price depth vector to get remaining trade size at each price depth
  remaining.pdepth <- lapply(1:length(price.depths), function(x)
  {
    remaining.price.depth (th.price.depth.output, price.depths[x])
  }
  )
  
  total.volume <- sum(sapply(th.price.depth.output[[2]], max))
  
  #create empty matrix to be filled
  output <- matrix(0, nrow =length(depths), ncol =length(price.depths))
  
  for (i in 1:length(depths))
  {
    for (j in 1:length(price.depths))
    {
      #use the remaining depth and price depth rows to calculate quadrant data
      output[i,j] <- quadrant(remaining.depth[[i]], remaining.pdepth[[j]])
    }
  }
  
  output <- output/total.volume
  
  return( output)
}

# get price depth and remaining volumes
# Python: trades_price_depth ()
th.price.depth <- function( th, market, i.limit)
{
  
  if( i.limit < 0 ) #min trade size in bitcoins
  {stop("i.limit must be a positive number")} 
  
  #order the trades in the order they come in
  th <- th[ order(th$ts, th$id), ]
  
  #filter trade data by selected market
  th <- th[which(th$m == market),]
  
  # This is to save computing time
  q <- th$q
  r <- th$r
  buy <- th$buy
  
  #get transactions into bins so that 1 bin = 1 trade because some trades are filled with multiple transactions
  split.th <- split(seq(nrow(th)), as.numeric(as.factor(paste(as.numeric(th$ts), buy))))
  
  #
  r1 <- lapply (1:length(split.th), function(x) { r[split.th[[x]]][order(r[split.th[[x]]])] })
  #
  q1 <- lapply (1:length(split.th), function(x) { q[split.th[[x]]][order(r[split.th[[x]]])] })
  
  #get price depths in numbers greater than 1
  price.depth <- sapply(1:length(split.th), function (x)
  {
    if (buy[ split.th[[x]][1] ] == TRUE)
    {
      1/(head(r1[[x]],1)/r1[[x]])
    }
    else
    {
      tail(r1[[x]],1)/r1[[x]]
    }
  })
    
  #remaining volume at each point along the trade
  remaining.volumes <- sapply(1:length(split.th), function (x) 
  {
    #rev(cumsum(rev(r[split.th[[x]]]*q[split.th[[x]]])))
    if (buy[ split.th[[x]][1] ] == TRUE)
    {
      remain <- rev(cumsum(rev(r1[[x]]*q1[[x]])))
    }
    else
    {
      remain <- rev(cumsum(r1[[x]]*q1[[x]]))
    }
    
    sapply(1:length(remain), function (x) 
    {
      if (remain[x] <= i.limit)
      {remain[x]}
      else
      {i.limit}
    })
  })

  return(list(price.depth, remaining.volumes))
}

remaining.price.depth <- function(th.price.depth.output, price.depth)
{
  pdepth <- th.price.depth.output[[1]]
  remain <- th.price.depth.output[[2]]
  
  remaining.pdepth <- sapply (1:length(pdepth), function (x)
  {
    if(any(price.depth <= pdepth[[x]]) )
    {
      remain[[x]][min(which(price.depth <= pdepth[[x]]))]
    }
    else
    {0}
  }
  )
}


#trade size remaining as a function of depth

remaining.size.depth <- function(th.price.depth.output, depth)
{
      volume <- sapply(th.price.depth.output[[2]], max)     
  
      remainder <- volume - depth
      #make sure if the reaminder is less than 0 we only have 0
      
      sapply(1:length(remainder), function(x)
      {
      if (remainder[x] <0)
      {0}
      else
      {remainder[x]}
      }
      )
}

#for use in heatmap
quadrant <- function (remaining.depth, remaining.pdepth)
{
  sum(pmin(remaining.depth, remaining.pdepth))
}