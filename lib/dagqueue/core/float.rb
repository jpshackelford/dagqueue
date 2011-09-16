# For 1.8.7 Compatibility
# Credit:  Gavin Kistner @ http://www.ruby-forum.com/topic/201424
if Float.instance_method(:round).arity == 0
  class Float
    alias_method :_orig_round, :round
    def round( decimals=0 )
      factor = 10**decimals
      (self*factor)._orig_round / factor.to_f
    end
  end
end
