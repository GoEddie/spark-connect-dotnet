using Apache.Arrow;
using Apache.Arrow.Types;

namespace ml;

public sealed class NullableChildListType : NestedType
{
    public override void Accept(IArrowTypeVisitor visitor)
    {
    }

    public override ArrowTypeId TypeId => ArrowTypeId.List;
        public override string Name => "list";
        
        public Field ValueField => Fields[0];

        public IArrowType ValueDataType => Fields[0].DataType;

        public NullableChildListType(Field valueField)
            : base(valueField) { }

        public NullableChildListType(IArrowType valueDataType)
            : this(new Field("item", valueDataType, true)) { }

        // public override void Accept(IArrowTypeVisitor visitor) => Accept(this, visitor);
    
}